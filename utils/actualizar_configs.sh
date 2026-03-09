
#!/bin/bash
set -e

bold=$(tput bold)
normal=$(tput sgr0)

fail() {
  echo -e "\n\nTry ${bold}'./actualizar_configs.sh --help'${normal} or ${bold}'./actualizar_configs.sh -h'${normal} for more information" >&2
  exit 255
}

if [[ "$*" =~ (^|\ )(-h|-H|--help)($|\ ) ]]; then
  echo "
${bold}NAME${normal}
    ${bold}actualizar_configs.sh${normal} - script to update configuration variables in TP modules.

${bold}SYNOPSIS${normal}
    ${bold}actualizar_configs.sh${normal} [ -p=module ... ] [ -c=key=value ... ] repository_path

${bold}DESCRIPTION${normal}
    Updates variables in .config and .cfg files inside specified modules.

${bold}OPTIONS${normal}
    ${bold}-p | --project${normal}      Module name (e.g., master, worker, storage).
    ${bold}-c | --config${normal}       Key-value pair to replace (e.g., IP_MASTER=192.168.0.10).

${bold}EXAMPLE${normal}
    ./actualizar_configs.sh -p=master -p=worker -p=storage -c=IP_MASTER=192.168.0.10 -c=IP_STORAGE=192.168.0.12 ../tp-2025-2c-So-for-dummies
"
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo -e "\n\n${bold}No repository path specified!${normal}" >&2
  fail
fi

MODULES=()
CONFIGURATIONS=()
OPTIONS=("${@:1:$#-1}")
for i in "${OPTIONS[@]}"; do
    case $i in
        -p=*|--project=*)
          MODULES+=("${i#*=}")
          shift
        ;;
        -c=*|--config=*)
          CONFIGURATIONS+=("${i#*=}")
          shift
        ;;
        *)
          echo -e "\n\n${bold}Invalid option:${normal} ${i}" >&2
          fail
        ;;
    esac
done

REPO_PATH="${!#}"
if [[ ! -d "$REPO_PATH" ]]; then
  echo -e "\n\n${bold}Invalid path${normal}: $REPO_PATH" >&2
  fail
fi

echo -e "\n\n${bold}Updating configs in:${normal} $REPO_PATH"
echo -e "${bold}Modules:${normal} ${MODULES[*]}"
echo -e "${bold}Configs:${normal} ${CONFIGURATIONS[*]}"

for module in "${MODULES[@]}"; do
  MODULE_PATH="$REPO_PATH/$module"
  if [[ ! -d "$MODULE_PATH" ]]; then
    echo -e "\n${bold}Warning:${normal} Module '$module' not found in $REPO_PATH"
    continue
  fi

  for config in "${CONFIGURATIONS[@]}"; do
    KEY="${config%=*}"
    VALUE="${config#*=}"
    echo -e "\nReplacing ${bold}${KEY}${normal} with ${bold}${VALUE}${normal} in module ${bold}${module}${normal}..."
    grep -Rl "^\s*$KEY\s*=" "$MODULE_PATH" | grep -E '\.config|\.cfg' | \
      xargs -r sed -i "s|^\($KEY\s*=\).*|\1$VALUE|"
  done
done

echo -e "\n\n${bold}Update done!${normal}\n"
