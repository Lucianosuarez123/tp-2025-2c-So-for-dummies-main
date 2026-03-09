#include "../includes/iniciarMaster.h"

void iniciar_Master(char* archivo_configuracion){
iniciar_config(archivo_configuracion);
iniciar_loggers();
lista_querys=list_create();
lista_workers=list_create();

// Listas por estado
cola_ready = list_create();
cola_exec = list_create();


// Mutex por estado
pthread_mutex_init(&mutex_ready, NULL);
pthread_mutex_init(&mutex_exec, NULL);
pthread_mutex_init(&mutex_exit, NULL);

// Semáforo que indica si hay queries en READY
sem_init(&sem_queries_en_ready, 0, 0);

// Semáforo que indica si hay al menos un Worker libre
sem_init(&sem_worker_libre, 0, 0);
sem_init(&sem_eventos, 0, 0);
pthread_mutex_init(&mutex_lista_querys, NULL);
pthread_mutex_init(&mutex_lista_workers, NULL);
pthread_mutex_init(&mutex_id_query, NULL);

}

void iniciar_config(char* archivo_config){
    char exe_path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (len == -1) {
        perror("No se pudo obtener la ruta del ejecutable");
        exit(EXIT_FAILURE);
    }
    exe_path[len] = '\0';

    // Obtener el directorio del ejecutable
    char* dir = dirname(exe_path);

    // Intentar con ./master.config
    char config_path[PATH_MAX];
    snprintf(config_path, sizeof(config_path), "%s/../../utils/tests/%s.config", dir, archivo_config);

    master_config = config_create(config_path);

    // Si no está, intentar con ../master.config
    if (master_config == NULL) {
        snprintf(config_path, sizeof(config_path), "%s/../../../utils/tests/%s.config", dir, archivo_config);

        master_config = config_create(config_path);
    }

    if (master_config == NULL) {
        perror("No se pudo cargar el archivo de configuración");
        exit(EXIT_FAILURE);
    }

    PUERTO_ESCUCHA = config_get_string_value(master_config, "PUERTO_ESCUCHA");
    ALGORITMO_PLANIFICACION = config_get_string_value(master_config, "ALGORITMO_PLANIFICACION");
    TIEMPO_AGING = config_get_int_value(master_config, "TIEMPO_AGING");
    LOG_LEVEL = config_get_string_value(master_config, "LOG_LEVEL");

    if(PUERTO_ESCUCHA == NULL || ALGORITMO_PLANIFICACION == NULL || TIEMPO_AGING < 0 || LOG_LEVEL == NULL){
    printf("Error al intentar cargar el config.");
    exit(EXIT_FAILURE);
  }

}
void iniciar_loggers() {

    remove("master.log");

    master_logger = log_create("master.log", "M_LOG", 1, log_level_from_string(LOG_LEVEL));

    if (master_logger == NULL) {
        perror("No se pudo crear o encontrar el archivo.");
        exit(EXIT_FAILURE);
    }
}
