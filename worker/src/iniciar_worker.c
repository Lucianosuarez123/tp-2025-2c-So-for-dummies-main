#include "../includes/iniciarWorker.h"


void inicializar_worker(){

    iniciar_config();
    iniciar_loggers();
    //tablas_de_paginas=dictionary_create();
    pthread_mutex_init(&mutex_hilo_ejecutar, NULL);
    pthread_mutex_init(&mutex_interrupcion, NULL);
    pthread_mutex_init(&mutex_resultado_global_lectura_bloque, NULL);
}

void iniciar_config(){
    char exe_path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (len == -1) {
        perror("No se pudo obtener la ruta del ejecutable");
        exit(EXIT_FAILURE);
    }
    exe_path[len] = '\0';

    // Obtener el directorio del ejecutable
    char* dir = dirname(exe_path);

    // Intentar con ./cpu.config
    char config_path[PATH_MAX];
    snprintf(config_path, sizeof(config_path), "%s/../../utils/tests/%s.config", dir, archivo_config);

    worker_config = config_create(config_path);

    // Si no está, intentar con ../cpu.config
    if (worker_config == NULL) {
        snprintf(config_path, sizeof(config_path), "%s/../../utils/tests/%s.config", dir, archivo_config);

        worker_config = config_create(config_path);
    }

    if (worker_config == NULL) {
        perror("No se pudo cargar el archivo de configuración");
        exit(EXIT_FAILURE);
    }

    IP_MASTER = config_get_string_value(worker_config, "IP_MASTER");
    PUERTO_MASTER = config_get_string_value(worker_config, "PUERTO_MASTER");
    IP_STORAGE = config_get_string_value(worker_config, "IP_STORAGE");
    PUERTO_STORAGE = config_get_string_value(worker_config, "PUERTO_STORAGE");
    LOG_LEVEL = config_get_string_value(worker_config, "LOG_LEVEL");

    TAM_MEMORIA = config_get_int_value(worker_config, "TAM_MEMORIA");
    RETARDO_MEMORIA = config_get_int_value(worker_config, "RETARDO_MEMORIA");
    ALGORITMO_REEMPLAZO = config_get_string_value(worker_config, "ALGORITMO_REEMPLAZO");
    PATH_SCRIPTS = config_get_string_value(worker_config, "PATH_SCRIPTS");

    if(IP_MASTER == NULL || PUERTO_MASTER == NULL || IP_STORAGE == NULL || PUERTO_STORAGE == NULL || LOG_LEVEL == NULL ||
       TAM_MEMORIA < 0 || RETARDO_MEMORIA < 0 || ALGORITMO_REEMPLAZO == NULL || PATH_SCRIPTS == NULL){
    printf("Error al intentar cargar el config.");
    exit(EXIT_FAILURE);}

}
void iniciar_loggers() {
    char* path_log = string_from_format("./%s.log", ID_Worker);
    remove(path_log);
    worker_logger = log_create(path_log, ID_Worker, 1, log_level_from_string(LOG_LEVEL));
    free(path_log);
    if (worker_logger == NULL) {
        perror("No se pudo crear o encontrar el archivo. (log worker_logger)");
        exit(EXIT_FAILURE);
    }
    log_debug_seguro(worker_logger,"logger iniciado correctamente");

}
