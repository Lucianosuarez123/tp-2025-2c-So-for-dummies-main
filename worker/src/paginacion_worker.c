#include "../includes/paginacion_worker.h"

void cargar_pagina(int query_id, char* file_tag, int numero_pagina) {
    log_debug_seguro(worker_logger, "## <%d> Estoy cargando la pagina ya que no se encontraba. con numero de pagina: %d con un file_tag: %s ", query_id, numero_pagina, file_tag);
    int marco_libre = bitarray_hay_marco_libre(bitmap_marcos)
                      ? buscar_marco_libre(bitmap_marcos)
                      : reemplazar_pagina(query_id, file_tag, numero_pagina);

    if (marco_libre == -1) {
        log_error_seguro(worker_logger, "No se pudo obtener marco libre para cargar página");
        return;
    }
    char* file_tag_dup = strdup(file_tag);
    char* file = strtok(file_tag_dup, ":");
    char* tag = strtok(NULL, ":");
    log_info_seguro(worker_logger, "Query <%d>: - Memoria Miss - File: <%s> - Tag: <%s> - Página: <%d>", query_id, file, tag, numero_pagina);
    char* contenido = pedir_contenido_storage(file_tag, numero_pagina);
    if (contenido == NULL) {
        log_error_seguro(worker_logger, "No se pudo obtener contenido del Storage para %s:%d", file_tag, numero_pagina);
        // Intentar limpiar la reserva del marco si fue reservada por buscar_marco_libre()
        if (marco_libre >= 0) {
            // Comprobar bajo los mutexes si alguna página ya tomó ese marco; si no, limpiarlo
            // Seguir el orden de locks: mutex_tablas -> mutex_memoria -> mutex_bitmap
            pthread_mutex_lock(&mutex_tablas);
            pthread_mutex_lock(&mutex_memoria);
            pthread_mutex_lock(&mutex_bitmap);

            bool ocupado_por_pagina = false;
            t_list* claves = dictionary_keys(tablas_de_paginas);
            for (int k = 0; k < list_size(claves) && !ocupado_por_pagina; k++) {
                char* key = list_get(claves, k);
                t_list* tabla = dictionary_get(tablas_de_paginas, key);
                if (tabla == NULL) continue;
                for (int i = 0; i < list_size(tabla); i++) {
                    t_pagina* p = list_get(tabla, i);
                    if (p->presente && p->marco == marco_libre) { ocupado_por_pagina = true; break; }
                }
            }
            list_destroy(claves);

            if (!ocupado_por_pagina) {
                bitarray_clean_bit(bitmap_marcos, marco_libre);
                log_debug_seguro(worker_logger, "cargar_pagina: reserva limpiada para marco %d (storage fail)", marco_libre);
            } else {
                log_debug_seguro(worker_logger, "cargar_pagina: reserva NO limpiada para marco %d (otro hilo la tomó)", marco_libre);
            }

            pthread_mutex_unlock(&mutex_bitmap);
            pthread_mutex_unlock(&mutex_memoria);
            pthread_mutex_unlock(&mutex_tablas);
        }
        return;
    }

    // NOTA: No hacemos memcpy hasta que asignar_marco_a_pagina confirme la
    // reserva del marco. Mantener `contenido` en memoria durante los intentos.

    // Buscar si ya existe una entrada para esta página
    t_list* tabla = NULL;
    pthread_mutex_lock(&mutex_tablas);
    tabla = dictionary_get(tablas_de_paginas, file_tag);
    if (tabla == NULL) {
        tabla = list_create();
        // duplicar la clave para evitar dangling pointer
        // Evitar duplicar dos veces la clave. `dictionary_put` (commons) realiza su propia copia
        // del key si lo requiere; pasar `file_tag` tal cual evita una posible doble-duplicación
        // que causaba un leak detectado por Valgrind.
        dictionary_put(tablas_de_paginas, file_tag, tabla);
        log_debug_seguro(worker_logger, "cargar_pagina: se creo tabla y dictionary_put para key %s", file_tag);
    }

        // Buscar dentro de la `tabla` ya obtenida (evitar llamar a buscar_pagina que toma el mutex de tablas otra vez)
        t_pagina* pagina = NULL;
        for (int i = 0; i < list_size(tabla); i++) {
            t_pagina* p = list_get(tabla, i);
            if (p->numero_pagina == numero_pagina) { pagina = p; break; }
        }
        if (pagina == NULL) {
            pagina = malloc(sizeof(t_pagina));
            pagina->numero_pagina = numero_pagina;
            pagina->en_reemplazo = false;
            pagina->file_tag=strdup(file_tag);
            list_add(tabla, pagina);
            log_debug_seguro(worker_logger, "cargar_pagina: se agrego pagina %d a la tabla (marco provisional)", numero_pagina);
        }
    pthread_mutex_unlock(&mutex_tablas);

    // Intentar asignar el marco de forma atómica; si otro hilo tomó el marco
    // entre tanto, reintentar buscar uno libre o ejecutar reemplazo.
    int intentos = 0;
    const int MAX_INTENTOS = 8;
    int asignado = -1;
    while (intentos < MAX_INTENTOS) {
        int res = asignar_marco_a_pagina(pagina, marco_libre,query_id,file_tag);
        if (res == 0) { asignado = marco_libre; break; }
        // falló porque el bit ya estaba ocupado; buscar otro marco libre
        intentos++;
        marco_libre = bitarray_hay_marco_libre(bitmap_marcos)
                      ? buscar_marco_libre(bitmap_marcos)
                      : reemplazar_pagina(query_id, file_tag, numero_pagina);
        if (marco_libre == -1) break;
    }

    if (asignado == -1) {
        log_error_seguro(worker_logger, "cargar_pagina: No se pudo asignar marco tras %d intentos", intentos);
        free(contenido);
        return;
    }
    log_debug_seguro(worker_logger, "cargar_pagina: metadatos actualizados para pagina %d -> marco %d", numero_pagina, asignado);

    usleep(RETARDO_MEMORIA * 1000);
    memcpy(memoria_interna + asignado * block_size, contenido, block_size);
    log_debug_seguro(worker_logger, "cargar_pagina: memcpy OK para marco %d", asignado);
    free(contenido);


    log_debug_seguro(worker_logger, "Query <%d>: - Memoria Add - File: <%s> - Tag: <%s> - Pagina: <%d> - Marco: <%d>", query_id, file, tag, numero_pagina, asignado);
    free(file_tag_dup);
}

// Helpers para actualizar bitmap y metadatos de página de forma atómica.
// Siempre siguen el orden de locking: mutex_tablas -> mutex_memoria -> mutex_bitmap
// Devuelve 0 si la asignación fue exitosa, -1 si el marco ya estaba ocupado.
int asignar_marco_a_pagina(t_pagina* pagina, int marco, int query_id_actual, char* file_tag) {
    pthread_mutex_lock(&mutex_tablas);
    pthread_mutex_lock(&mutex_memoria);
    pthread_mutex_lock(&mutex_bitmap);

    // Si otro hilo ya reservó el marco, fallar para reintentar con otro
    if (bitarray_test_bit(bitmap_marcos, marco)) {
        pthread_mutex_unlock(&mutex_bitmap);
        pthread_mutex_unlock(&mutex_memoria);
        pthread_mutex_unlock(&mutex_tablas);
        return -1;
    }

    bitarray_set_bit(bitmap_marcos, marco);
    char* file_tag_dup = strdup(file_tag);
    char* file = strtok(file_tag_dup, ":");
    char* tag = strtok(NULL, ":");
    //log_debug_seguro(worker_logger, "asignar_marco_a_pagina: set bitmap marco %d por tid %ld para pagina %d", marco, (long)pthread_self(), pagina->numero_pagina);
    log_info_seguro(worker_logger, "Query <%d>: Se asigna el Marco: <%d> a la Página: <%d> perteneciente al - File: <%s> - Tag: <%s>", query_id_actual, marco, pagina->numero_pagina, file, tag);

    pagina->marco = marco;
    pagina->modificada = false;
    pagina->presente = true;
    pagina->timestamp = get_timestamp();
    pagina->uso = true;
    free(file_tag_dup);
    pthread_mutex_unlock(&mutex_bitmap);
    pthread_mutex_unlock(&mutex_memoria);
    pthread_mutex_unlock(&mutex_tablas);
    return 0;
}

void liberar_marco_de_pagina(t_pagina* pagina, int marco, int query_id, char* file_tag) {
    pthread_mutex_lock(&mutex_tablas);
    pthread_mutex_lock(&mutex_memoria);
    pthread_mutex_lock(&mutex_bitmap);


    bitarray_clean_bit(bitmap_marcos, marco);
    char* file_tag_dup = strdup(file_tag);
    char* file = strtok(file_tag_dup, ":");
    char* tag = strtok(NULL, ":");
    //log_debug_seguro(worker_logger, "liberar_marco_de_pagina: limpiado bitmap marco %d por tid %ld para pagina %d", marco, (long)pthread_self(), pagina->numero_pagina);
    log_info_seguro(worker_logger, "Query <%d> : Se libera el Marco: <%d>  perteneciente al - File: <%s> - Tag: <%s>  ", query_id, marco, file, tag);

    pagina->presente = false;
    pagina->marco = -1;
    pagina->en_reemplazo = false;

    pthread_mutex_unlock(&mutex_bitmap);
    pthread_mutex_unlock(&mutex_memoria);
    pthread_mutex_unlock(&mutex_tablas);
    free(file_tag_dup);
}

t_pagina* buscar_pagina(char* file_tag, int numero_pagina) {
    pthread_mutex_lock(&mutex_tablas);
    t_list* tabla = dictionary_get(tablas_de_paginas, file_tag);
    if (tabla == NULL) {
        pthread_mutex_unlock(&mutex_tablas);
        return NULL;
    }

    t_pagina* resultado = NULL;
    for (int i = 0; i < list_size(tabla); i++) {
        t_pagina* pagina = list_get(tabla, i);
        if (pagina->numero_pagina == numero_pagina) {
            resultado = pagina;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_tablas);
    return resultado;
}
// Unsafe variant: assumes caller holds mutex_tablas.
t_pagina* buscar_pagina_por_marco_unsafe(int marco) {
    t_pagina* resultado = NULL;
    log_debug_seguro(worker_logger, "digo que resultado es null");

    t_list* claves = dictionary_keys(tablas_de_paginas);
    for (int k = 0; k < list_size(claves); k++) {
        char* key = list_get(claves, k);
        t_list* tabla = dictionary_get(tablas_de_paginas, key);
        if (tabla == NULL) continue;
        for (int i = 0; i < list_size(tabla); i++) {
            t_pagina* pagina = list_get(tabla, i);
            if (pagina->presente && !pagina->en_reemplazo && pagina->marco == marco) {
                resultado = pagina;
                break;
            }
        }
        if (resultado != NULL) break;
    }
    list_destroy(claves);
    return resultado;
}

// Safe wrapper: toma mutex_tablas internamente.
t_pagina* buscar_pagina_por_marco(int marco) {
    pthread_mutex_lock(&mutex_tablas);
    t_pagina* res = buscar_pagina_por_marco_unsafe(marco);
    pthread_mutex_unlock(&mutex_tablas);
    return res;
}

// Verifica la consistencia entre bitmap de marcos y tablas de páginas.
// Toma internamente los mutexes en el orden: mutex_tablas -> mutex_bitmap
// y nunca llama a funciones que vuelvan a bloquear `mutex_tablas`.
void verificar_consistencia_bitmap_tablas() {
    pthread_mutex_lock(&mutex_tablas);
    pthread_mutex_lock(&mutex_bitmap);

    for (int j = 0; j < cantidad_marcos; j++) {
        int bit = bitarray_test_bit(bitmap_marcos, j) ? 1 : 0;

        // Buscar manualmente una página que apunte al marco j
        t_pagina* encontrada = NULL;
        t_list* claves = dictionary_keys(tablas_de_paginas);
        for (int k = 0; k < list_size(claves) && encontrada == NULL; k++) {
            char* key = list_get(claves, k);
            t_list* tabla = dictionary_get(tablas_de_paginas, key);
            if (tabla == NULL) continue;
            for (int i = 0; i < list_size(tabla); i++) {
                t_pagina* pagina = list_get(tabla, i);
                if (pagina->presente && pagina->marco == j) { encontrada = pagina; break; }
            }
        }
        list_destroy(claves);

        if (bit && encontrada == NULL) {
            log_warning_seguro(worker_logger, "Inconsistencia: bitmap[%d]=1 pero no existe pagina con marco %d", j, j);
        } else if (!bit && encontrada != NULL) {
            log_warning_seguro(worker_logger, "Inconsistencia: bitmap[%d]=0 pero pagina %d presente apunta al marco", j, encontrada->numero_pagina);
        }
    }

    pthread_mutex_unlock(&mutex_bitmap);
    pthread_mutex_unlock(&mutex_tablas);
}