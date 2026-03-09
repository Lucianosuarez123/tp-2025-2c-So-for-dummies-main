#include <../includes/mainWorker.h>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s [archivo_config] [ID_Worker]\n", argv[0]);
        return 1;
    }

    archivo_config = argv[1];
    ID_Worker= argv[2];
    inicializar_worker();

    //nos conectamos con los otros modulos

    // Conexiones
    fd_master = crear_conexion_cliente(IP_MASTER, PUERTO_MASTER);
    fd_storage = crear_conexion_cliente(IP_STORAGE, PUERTO_STORAGE);
    interrupcion.hay_interrupcion=false;
    interrupcion.query_id=-1;
    // Handshakes
    enviar_id(fd_master, ID_Worker);
    handshake_con_storage(fd_storage, ID_Worker);
    inicializar_memoria_interna();
    // Lanzamos el hilo que atiende al master
    pthread_t hilo_worker_master;
    pthread_create(&hilo_worker_master, NULL, (void*) atender_worker_master, NULL);
    pthread_join(hilo_worker_master, NULL); // Esperamos que termine

    //* probablemente haya que crear un hilo para que el worker quede escuchando mensajes de master

    //free(memoria_interna);
    //bitarray_destroy(bitmap_marcos);
    //dictionary_destroy_and_destroy_elements(tablas_de_paginas, (void*) list_destroy_and_destroy_elements);
    return 0;
}

void handshake_con_storage(int fd_storage, char* ID_Worker){
    pthread_mutex_lock(&mutex_fd_storage);
    enviar_id(fd_storage, ID_Worker);
    int cod_op= recibir_operacion(fd_storage);
    if(cod_op==ENVIAR_TAM_BLOQUE_STORAGE_WORKER){
        t_buffer* buffer= recibir_buffer(fd_storage);
        if(buffer== NULL){
            log_error_seguro(worker_logger, "error al recibir buffer para obtener work size");
        }
        block_size= extraer_int(buffer);
        liberar_buffer(buffer);
        log_debug_seguro(worker_logger,"block size=%d",block_size);

    }
    else log_error_seguro(worker_logger,"storage no mando el block size");
    pthread_mutex_unlock(&mutex_fd_storage);
}

void enviar_id(int socket, char* id_worker){
    t_buffer* buffer=crear_buffer();
    agregar_string_a_buffer(buffer, id_worker);
    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(ENVIAR_ID_WORKER, buffer);
    enviar_paquete(paquete, socket);
    liberar_paquete(paquete);
}