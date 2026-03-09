#include <../includes/mainStorage.h>

int main(int argc, char* argv[]) {
        if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config] \n", argv[0]);
        return 1;
    }
    archivo_config = argv[1];
    inicializar_storage();

    puerto_escucha_fd = iniciar_servidor(PUERTO_ESCUCHA, storage_logger);

    // para poder conectar multiples workers por el mismo puerto y luego atenderlos por separado
    pthread_t hilo;
    pthread_create(&hilo, NULL, gestionar_conexiones, NULL);
    pthread_join(hilo, NULL);
    //liberar_recursos(); // por si termina naturalmente

    return 0;
}

void* gestionar_conexiones(void* _) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    pthread_cleanup_push((void (*)(void*))close, (void*)(intptr_t)puerto_escucha_fd); // cierra el socket si se cancela

    while (1) {
        int nuevo_cliente_fd = esperar_cliente(puerto_escucha_fd, storage_logger);
        if (nuevo_cliente_fd < 0) {
            log_error_seguro(storage_logger, "Error al aceptar cliente.");
            continue;
        }

        int cod_op = recibir_operacion(nuevo_cliente_fd);

        if (cod_op == ENVIAR_ID_WORKER) { // WORKER

            t_buffer* buffer = recibir_buffer(nuevo_cliente_fd);
            if(buffer==NULL){
                log_error_seguro(storage_logger, "Error al extraer buffer para nuevo WORKER.");
                close(nuevo_cliente_fd);
                pthread_exit(NULL);
            }
            char* id_worker = extraer_string(buffer);
            liberar_buffer(buffer);
            t_worker* nuevo_worker=crear_worker(nuevo_cliente_fd, id_worker);
            if(nuevo_worker==NULL){
                log_error_seguro(storage_logger,"Error al crear estructura para worker conectado");
                continue;
            }
            pthread_mutex_lock(&mutex_lista_workers);
            log_info_seguro(storage_logger,"##Se conecta el Worker <%s> - Cantidad de Workers: <%d>", nuevo_worker->id_worker, list_size(lista_workers));
            pthread_mutex_unlock(&mutex_lista_workers);

            pthread_t hilo_worker;
            pthread_create(&hilo_worker, NULL, atender_storage_worker, nuevo_worker);
        
            enviar_tam_bloque(nuevo_worker);

        }

        pthread_testcancel(); // punto seguro de cancelación
    }

    pthread_cleanup_pop(0); // no cerrar el socket si termina normalmente
    return NULL;
}

t_worker* crear_worker(int socket, char* id_worker){
    t_worker* nuevo_worker=malloc(sizeof(t_worker));
    if(nuevo_worker==NULL){
        log_error_seguro(storage_logger, "fallo el malloc para crear al worker");
        return NULL;
    }
    nuevo_worker->socket=socket;
    nuevo_worker->id_worker=id_worker;

    pthread_mutex_lock(&mutex_lista_workers);
    list_add(lista_workers, nuevo_worker);
    pthread_mutex_unlock(&mutex_lista_workers);
    return nuevo_worker;
}
