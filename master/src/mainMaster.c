#include <../includes/mainMaster.h>

void liberar_recursos() {
    list_destroy_and_destroy_elements(lista_workers, (void*)destruir_worker);
    log_destroy(master_logger);
    config_destroy(master_config);
    close(puerto_escucha_fd);
    // destruir semáforos y mutex si corresponde
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return 1;
    }
    archivo_config = argv[1];
    iniciar_Master(archivo_config);

    puerto_escucha_fd = iniciar_servidor(PUERTO_ESCUCHA, master_logger);
    pthread_t hilo_conexiones;
    pthread_create(&hilo_conexiones, NULL, gestionar_conexiones, NULL);
    pthread_detach(hilo_conexiones);
    log_debug_seguro(master_logger, "hilo de conexiones satisfactorio");


    pthread_t hilo_planificador;
    if(strcmp(ALGORITMO_PLANIFICACION,"FIFO")==0){
        pthread_create(&hilo_planificador, NULL, planificador_fifo, NULL);
    }
    else{
        pthread_create(&hilo_planificador, NULL, planificador_con_desalojo, NULL);
    }
    log_debug_seguro(master_logger, "hilo de planificador iniciado");

    pthread_join(hilo_planificador, NULL);
    liberar_recursos();

    return 0;
}

void* gestionar_conexiones(void* _) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    pthread_cleanup_push((void (*)(void*))close, (void*)(intptr_t)puerto_escucha_fd); // cierra el socket si se cancela

    while (1) {
        int nuevo_cliente_fd = esperar_cliente(puerto_escucha_fd, master_logger);
        if (nuevo_cliente_fd < 0) {
            log_error_seguro(master_logger, "Error al aceptar cliente.");
            continue;
        }

        int cod_op = recibir_operacion(nuevo_cliente_fd);

        if (cod_op == ENVIAR_ID_WORKER) { // WORKER

            t_buffer* buffer = recibir_buffer(nuevo_cliente_fd);
            if(buffer==NULL){
                log_error_seguro(master_logger, "Error al extraer buffer para nuevo WORKER.");
                close(nuevo_cliente_fd);
                pthread_exit(NULL);
            }
            char* id_worker = extraer_string(buffer);
            liberar_buffer(buffer);
            t_worker* nuevo_worker = malloc(sizeof(t_worker));
            if (nuevo_worker == NULL) {
                log_error_seguro(master_logger, "Error al asignar master para nuevo WORKER.");
                close(nuevo_cliente_fd);
                pthread_exit(NULL);
            }


            nuevo_worker->socket = nuevo_cliente_fd;
            nuevo_worker->id_worker=id_worker;
            nuevo_worker->query_asignada=NULL;
            nuevo_worker->ocupado=false;
            pthread_mutex_lock(&mutex_lista_workers);
            list_add(lista_workers, nuevo_worker);
            log_info_seguro(master_logger,"## Se conecta el Worker <%s> - Cantidad total de Workers: <%d>", nuevo_worker->id_worker, list_size(lista_workers));
            pthread_mutex_unlock(&mutex_lista_workers);
            sem_post(&sem_worker_libre);
            sem_post(&sem_eventos);




            pthread_t hilo_worker;
            pthread_create(&hilo_worker, NULL, atender_master_worker, nuevo_worker);

        } else if(cod_op==ENVIAR_QUERY_A_MASTER) { // Query
            t_buffer* buffer = recibir_buffer(nuevo_cliente_fd);
            if(buffer==NULL){
                log_error_seguro(master_logger, "Error al extraer buffer para nuevo QUERY.");
                close(nuevo_cliente_fd);
                pthread_exit(NULL);
            }
            int prioridad= extraer_int(buffer);
            char* archivo_query = extraer_string(buffer);
            liberar_buffer(buffer);



            t_query* nuevo_query= crear_query(nuevo_cliente_fd, archivo_query, prioridad);

            pthread_mutex_lock(&mutex_lista_workers);
            pthread_mutex_lock(&nuevo_query->mutex_query);
            log_info_seguro(master_logger,"## Se conecta un Query Control para ejecutar la Query <%s> con prioridad <%d> - Id asignado: <%d>. Nivel multiprocesamiento <%d>",nuevo_query->archivo_query, nuevo_query->prioridad, nuevo_query->id_query, list_size(lista_workers));
            pthread_mutex_unlock(&mutex_lista_workers);
            pthread_mutex_unlock(&nuevo_query->mutex_query);
            pthread_t hilo_query;
            pthread_create(&hilo_query, NULL, atender_master_query, nuevo_query);
            free(archivo_query);
            

        }

        pthread_testcancel(); // punto seguro de cancelación
    }

    pthread_cleanup_pop(0); // no cerrar el socket si termina normalmente

    list_destroy_and_destroy_elements(lista_workers, (void*)destruir_worker);

    return NULL;
}



