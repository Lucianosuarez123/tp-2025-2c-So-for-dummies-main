#include "../includes/gestion_memoria_worker.h"

pthread_mutex_t mutex_memoria = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_bitmap = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_tablas = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_fd_storage = PTHREAD_MUTEX_INITIALIZER;

void inicializar_memoria_interna() {
    tablas_de_paginas = dictionary_create();

    memoria_interna = malloc(TAM_MEMORIA);
    if (memoria_interna == NULL) {
        log_error_seguro(worker_logger, "No se pudo reservar memoria interna");
        exit(EXIT_FAILURE);
    }

    cantidad_marcos = TAM_MEMORIA / block_size;
    // bitarray expects a buffer of bytes; reservar ceil(cantidad_marcos/8) bytes
    int bytes_bitmap = (cantidad_marcos + 7) / 8;
    char* bitmap_buffer = calloc(bytes_bitmap, sizeof(char));
    bitmap_marcos = bitarray_create_with_mode(bitmap_buffer, cantidad_marcos, LSB_FIRST);

    log_debug_seguro(worker_logger, "Memoria interna inicializada con %d marcos de %d bytes", cantidad_marcos, block_size);
}

int buscar_marco_libre(t_bitarray* bitmap) {
    log_debug_seguro(worker_logger, "buscando marco libre...");
    pthread_mutex_lock(&mutex_bitmap);
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!bitarray_test_bit(bitmap, i)) {
            // Devolver un índice libre SÓLO para intentar reservación después.
            pthread_mutex_unlock(&mutex_bitmap);
            return i;
        }
    }
    pthread_mutex_unlock(&mutex_bitmap);
    return -1; // No hay marcos libres
}
bool bitarray_hay_marco_libre(t_bitarray* bitmap) {
    pthread_mutex_lock(&mutex_bitmap);
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!bitarray_test_bit(bitmap, i)){
            log_debug_seguro(worker_logger,"se encontro marco libre. marco");
            pthread_mutex_unlock(&mutex_bitmap);
            return true;
        };
    }
    pthread_mutex_unlock(&mutex_bitmap);
    return false;
}
uint64_t get_timestamp() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)(tv.tv_sec * 1000 + tv.tv_usec / 1000); // milisegundos
}



char* leer_memoria(int query_id, int marco, int offset, int tamanio) {
    if (marco < 0 || marco >= cantidad_marcos || offset + tamanio > block_size) {
        log_error_seguro(worker_logger, "Lectura inválida: marco %d, offset %d, tamaño %d", marco, offset, tamanio);
        return NULL;
    }

    usleep(RETARDO_MEMORIA * 1000);
    log_debug_seguro(worker_logger,"procedo a reservar memoria y a copiar el resultado de la memoria interna");
    char* resultado = malloc(tamanio + 1);
    pthread_mutex_lock(&mutex_memoria);
    memcpy(resultado, memoria_interna + marco * block_size + offset, tamanio);
    pthread_mutex_unlock(&mutex_memoria);
    resultado[tamanio] = '\0';
    log_debug_seguro(worker_logger,"ya hice eso y agregue el barra0");
    // buscar_pagina_por_marco toma mutex_tablas internamente
    t_pagina* pagina = buscar_pagina_por_marco(marco);
    if (pagina != NULL) {
        pthread_mutex_lock(&mutex_memoria);
        pagina->timestamp = get_timestamp();
        pagina->uso = true;
        pthread_mutex_unlock(&mutex_memoria);
    }
    log_debug_seguro(worker_logger,"estoy por poner log de lo que lei anashe");
    log_info_seguro(worker_logger, "Query <%d>: Acción: LEER - Dirección Física: <%d> - Valor: <%s>", query_id, marco * block_size + offset, resultado);

    return resultado;
}

void escribir_memoria(int query_id, int marco, int offset, char* contenido) {
    int tamanio = strlen(contenido);
    if (marco < 0 || marco >= cantidad_marcos || offset + tamanio > block_size) {
        log_error_seguro(worker_logger, "Escritura inválida: marco %d, offset %d, tamaño %d", marco, offset, tamanio);
        return;
    }

    usleep(RETARDO_MEMORIA * 1000);

    pthread_mutex_lock(&mutex_memoria);
    memcpy(memoria_interna + marco * block_size + offset, contenido, tamanio);
    pthread_mutex_unlock(&mutex_memoria);

    // Actualizar metadatos de la página asociada si existe
    // buscar_pagina_por_marco toma mutex_tablas internamente
    t_pagina* pagina = buscar_pagina_por_marco(marco);
    if (pagina != NULL) {
        pthread_mutex_lock(&mutex_memoria);
        pagina->modificada = true;
        pagina->timestamp = get_timestamp();
        pagina->uso = true;
        pthread_mutex_unlock(&mutex_memoria);
    }

    log_info_seguro(worker_logger, "Query <%d>: Acción: ESCRIBIR - Dirección Física: <%d> - Valor: <%s>", query_id, marco * block_size + offset, contenido);
}

void dump_memoria() {
    log_debug_seguro(worker_logger, "=== Dump de Memoria ===");

    void mostrar_tabla(char* key, void* value) {
        t_list* tabla = (t_list*)value;
        for (int i = 0; i < list_size(tabla); i++) {
            t_pagina* pagina = list_get(tabla, i);
            char* contenido = NULL;
            if (pagina->presente && pagina->marco >= 0) {
                contenido = malloc(block_size + 1);
                pthread_mutex_lock(&mutex_memoria);
                memcpy(contenido, memoria_interna + pagina->marco * block_size, block_size);
                pthread_mutex_unlock(&mutex_memoria);
                contenido[block_size] = '\0';
            }

            log_debug_seguro(worker_logger,
                "FileTag: %s | Página: %d | Marco: %d | Modificada: %s | Uso: %s | En_Reemplazo: %s | Timestamp: %llu | Contenido: %s",
                key,
                pagina->numero_pagina,
                pagina->marco,
                pagina->modificada ? "Sí" : "No",
                pagina->uso ? "Sí" : "No",
                pagina->en_reemplazo ? "Sí" : "No",
                pagina->timestamp,
                contenido ? contenido : "(sin marco) ");

            if (contenido) free(contenido);
        }
    }

    safe_dictionary_iterator(tablas_de_paginas, mostrar_tabla);
}
