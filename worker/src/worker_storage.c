#include <../includes/worker_storage.h>

bool flush_pagina(char* file_tag, t_pagina* pagina, int id_query) {

    if (!memoria_interna) {
        log_error_seguro(worker_logger, "Flush fallido: memoria_interna no inicializada");
        t_buffer* buffer_end = crear_buffer();
        agregar_int_a_buffer(buffer_end, query_id_actual);
        agregar_string_a_buffer(buffer_end, "FALLO_MEMORIA_INTERNA_NO_INICIALIZADA");
        t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
        enviar_paquete(paquete_end, fd_master);
        liberar_paquete(paquete_end);
        return false;
    }

    if (pagina == NULL || !pagina->presente || pagina->marco < 0) {
        log_error_seguro(worker_logger, "Flush fallido: página inválida o sin marco (page=%p, presente=%d, marco=%d)", pagina, pagina ? pagina->presente : 0, pagina ? pagina->marco : -1);
        t_buffer* buffer_end = crear_buffer();
        agregar_int_a_buffer(buffer_end, query_id_actual);
        agregar_string_a_buffer(buffer_end, "FALLO_PAGINA_INVALIDA_O_SIN_MARCO");
        t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
        enviar_paquete(paquete_end, fd_master);
        liberar_paquete(paquete_end);
        return false;
    }

    char* contenido = malloc(block_size + 1); // +1 para el '\0'
    pthread_mutex_lock(&mutex_memoria);
    memcpy(contenido, memoria_interna + pagina->marco * block_size, block_size);
    pthread_mutex_unlock(&mutex_memoria);
    contenido[block_size] = '\0';


    char** partes = string_split(file_tag, ":");
    if (!partes || !partes[0] || !partes[1]) {
        log_error_seguro(worker_logger, "Flush fallido: file_tag inválido (%s)", file_tag);
        free(contenido);

        t_buffer* buffer_end = crear_buffer();
        agregar_int_a_buffer(buffer_end, query_id_actual);
        agregar_string_a_buffer(buffer_end, "FALLO_FAIL_TAG_NO_VALIDO");
        t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
        enviar_paquete(paquete_end, fd_master);
        liberar_paquete(paquete_end);
        return false;
    }

    char* nombre_file = strdup(partes[0]);
    char* tag = strdup(partes[1]);

    t_buffer* buffer = crear_buffer();
    agregar_int_a_buffer(buffer, id_query);
    agregar_string_a_buffer(buffer, nombre_file);
    agregar_string_a_buffer(buffer, tag);
    agregar_int_a_buffer(buffer, pagina->numero_pagina);
    agregar_string_a_buffer(buffer, contenido);

    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_WRITE, buffer);
    pthread_mutex_lock(&mutex_fd_storage);
    enviar_paquete(paquete, fd_storage);
    liberar_paquete(paquete);
    free(contenido);
    log_debug_seguro(worker_logger,"envie pagina nro %d para flush", pagina->numero_pagina);
    int cod_respuesta = recibir_operacion(fd_storage);
    log_debug_seguro(worker_logger, "flush: cod_respuesta recibido=%d para pagina %d", cod_respuesta, pagina->numero_pagina);

    bool retorno=false;

    if (cod_respuesta == RESPUESTA_RESULTADO) {
        t_buffer* buffer_respuesta = recibir_buffer(fd_storage);
        char* resultado = extraer_string(buffer_respuesta);

        if (strcmp(resultado,"OPERACION EXITOSA")==0) {
            log_debug_seguro(worker_logger, "Flush exitoso: %s - Página %d", file_tag, pagina->numero_pagina);
            pthread_mutex_lock(&mutex_memoria);
            pagina->modificada = false;
            pthread_mutex_unlock(&mutex_memoria);
            retorno=true;
        } else {
            log_error_seguro(worker_logger, "Flush fallido: %s - Página %d", file_tag, pagina->numero_pagina);
            t_buffer* buffer_end = crear_buffer();
            agregar_int_a_buffer(buffer_end, query_id_actual);
            agregar_string_a_buffer(buffer_end, resultado);
            t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
            enviar_paquete(paquete_end, fd_master);
            liberar_paquete(paquete_end);
            retorno=false;
        }

        liberar_buffer(buffer_respuesta);
        free(resultado);
    } else {
        log_error_seguro(worker_logger, "Flush fallido: %s - Página %d", file_tag, pagina->numero_pagina);

        t_buffer* buffer_end = crear_buffer();
        agregar_int_a_buffer(buffer_end, query_id_actual);
        agregar_string_a_buffer(buffer_end, "FALLO_RESPUESTA_COD_OP_NO_COINCIDE_CON_EL_ESPERADO");
        t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
        enviar_paquete(paquete_end, fd_master);
        liberar_paquete(paquete_end);
        retorno=false;
    }

    pthread_mutex_unlock(&mutex_fd_storage);

    string_array_destroy(partes);
    free(nombre_file);
    free(tag);
    return retorno;
}

char* pedir_contenido_storage(char* file_tag, int numero_pagina) {
    // Armar buffer con datos

    char** partes = string_split(file_tag, ":");
    if (partes == NULL || partes[0] == NULL || partes[1] == NULL) {
        log_error_seguro(worker_logger, "pedir_contenido_storage: file_tag inválido: %s", file_tag);
        if (partes) string_array_destroy(partes);
        return NULL;
    }

    char* nombre_file = strdup(partes[0]);
    char* tag = strdup(partes[1]);
    t_buffer* buffer = crear_buffer();
    agregar_int_a_buffer(buffer, query_id_actual);
    agregar_string_a_buffer(buffer, nombre_file);
    agregar_string_a_buffer(buffer, tag);
    agregar_int_a_buffer(buffer, numero_pagina);

    // Crear paquete con código de operación
    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_PEDIR_PAGINA, buffer);
    pthread_mutex_lock(&mutex_fd_storage);
    enviar_paquete(paquete, fd_storage);
    liberar_paquete(paquete);
    log_debug_seguro(worker_logger, "## esperando por ti >3");
    // Esperar respuesta
    int cod_respuesta = recibir_operacion(fd_storage);
    log_debug_seguro(worker_logger, "pedir_contenido_storage: cod_respuesta recibido=%d para %s:%d", cod_respuesta, file_tag, numero_pagina);

    if (cod_respuesta != RESPUESTA_PAGINA) {
        log_error_seguro(worker_logger, "Storage respondió código inesperado al pedir página de %s:%d", file_tag, numero_pagina);
        pthread_mutex_unlock(&mutex_fd_storage);
        // liberar temporales
        free(nombre_file);
        free(tag);
        string_array_destroy(partes);
        return NULL;
    }

    // Recibir contenido
    log_debug_seguro(worker_logger, "pedir_contenido_storage: llamando recibir_buffer para %s:%d", file_tag, numero_pagina);
    t_buffer* buffer_respuesta = recibir_buffer(fd_storage);
    if (buffer_respuesta == NULL) {
        log_error_seguro(worker_logger, "Error al recibir contenido de Storage (buffer NULL) para %s:%d", file_tag, numero_pagina);
        pthread_mutex_unlock(&mutex_fd_storage);
        free(nombre_file);
        free(tag);
        string_array_destroy(partes);
        return NULL;
    }
    //log_debug_seguro(worker_logger, "pedir_contenido_storage: recibir_buffer OK size=%d para %s:%d", buffer_respuesta->size, file_tag, numero_pagina);
    //EXTRAER INT Y MENSAJE
    int resultado = extraer_int(buffer_respuesta);
    char* contenido = extraer_string(buffer_respuesta);
    //PROBABLEMENTE HAYA QUE MOVER EL LIBERAR BUFFER
    liberar_buffer(buffer_respuesta);
    pthread_mutex_lock(&mutex_resultado_global_lectura_bloque);
    respuesta_global_de_lectura_bloque.resultado=resultado;
    respuesta_global_de_lectura_bloque.contenido=contenido;
    pthread_mutex_unlock(&mutex_resultado_global_lectura_bloque);
    //CHECKEAR SI EL RESULTADO FUE 1=OK O 0=FALLO
    //SI FALLO, CAMBIO VARIABLE GLOBAL A 0 Y EL MENSAJE SERA EL CONTENIDO QUE PASO EL BUFFER RESPUESTA. ADEMAS RETORNAMOS NULL
    if(resultado!=1){
        //STORAGE MANDO UN ERROR AL REALIZAR ALGUNA COSA MIENTRAS LEIA EL BLOQUE
        //free(contenido);
        pthread_mutex_unlock(&mutex_fd_storage);
        free(nombre_file);
        free(tag);
        string_array_destroy(partes);
        return NULL;
    }
    //SI RESULTADO OK, CONTINUO CON ESTO DE VALIDAR TAMAÑO
    // Validar tamaño
    if (strlen(contenido) != block_size) {
        log_error_seguro(worker_logger, "Contenido recibido de Storage no coincide con block_size (%d bytes)", block_size);
        free(contenido);
        pthread_mutex_unlock(&mutex_fd_storage);
        free(nombre_file);
        free(tag);
        string_array_destroy(partes);
        return NULL;
    }

    pthread_mutex_unlock(&mutex_fd_storage);
    // liberar duplicados temporales
    free(nombre_file);
    free(tag);
    string_array_destroy(partes);

    return contenido;
}