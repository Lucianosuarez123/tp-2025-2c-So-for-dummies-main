#include "../includes/algoritmos_reemplazo.h"
#include "../includes/paginacion_worker.h"




int reemplazar_pagina(int query_id, char* file_tag, int nro_pagina_a_colocar_en_memoria) {
    t_pagina* victima = seleccionar_pagina_para_reemplazo();
    if (victima == NULL) {
        log_error_seguro(worker_logger, "No se encontró página para reemplazo");
        return -1;
    }
        int marco_liberado = victima->marco;
         log_info_seguro(worker_logger, "## Query <%d>: Se reemplaza la página <%s>/<%d> por la <%s>/<%d>",query_id, victima->file_tag, victima->numero_pagina,file_tag, nro_pagina_a_colocar_en_memoria);
        if (victima->modificada) {
            // Flush fuera de locks de tablas/memoria; flush_pagina protege acceso a memoria internamente
            if(!flush_pagina(file_tag, victima, query_id)) {
                log_error_seguro(worker_logger,"fallo al realizar flush de pagina %d",victima->numero_pagina);
                return -1;}
        }

        // Actualizar bitmap y metadatos de la página de forma atómica
        liberar_marco_de_pagina(victima, marco_liberado, query_id, file_tag);


        return marco_liberado;
    }

t_pagina* seleccionar_pagina_para_reemplazo() {
    if (string_equals_ignore_case(ALGORITMO_REEMPLAZO, "CLOCK-M")) {
        log_debug_seguro(worker_logger, "Algoritmo de reemplazo seleccionado: %s", ALGORITMO_REEMPLAZO);
        return seleccionar_por_clock_modificado();
    } else if (string_equals_ignore_case(ALGORITMO_REEMPLAZO, "LRU")) {
        return seleccionar_por_lru();
    } else {
        log_error_seguro(worker_logger, "Algoritmo de reemplazo desconocido: %s", ALGORITMO_REEMPLAZO);
        return NULL;
    }
}



t_pagina* seleccionar_por_clock_modificado() {
    if (cantidad_marcos <= 0) return NULL;

    const int start = puntero_clock % cantidad_marcos;

    int _old_cancel_state;
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &_old_cancel_state);

    // Orden consistente: tablas -> memoria -> bitmap
    pthread_mutex_lock(&mutex_tablas);
    pthread_mutex_lock(&mutex_memoria);
    pthread_mutex_lock(&mutex_bitmap);

    // =========================================================
    // PASADA 1 (P1): seleccionar la primera U=0 && M=0
    // =========================================================
    for (int i = 0; i < cantidad_marcos; i++) {
        const int idx = (start + i) % cantidad_marcos;
        if (!bitarray_test_bit(bitmap_marcos, idx)) {
            log_debug_seguro(worker_logger, "CLOCK-M [P1]: marco %d -> libre", idx);
            continue;
        }
        t_pagina* pagina = buscar_pagina_por_marco_unsafe(idx);
        if (!pagina) {
            log_debug_seguro(worker_logger, "CLOCK-M [P1]: marco %d -> bitmap=1 sin pagina", idx);
            continue;
        }
        log_debug_seguro(worker_logger,
            "CLOCK-M [P1]: marco %d -> pag %d | U=%d M=%d en_rep=%d",
            idx, pagina->numero_pagina, pagina->uso, pagina->modificada, pagina->en_reemplazo);

        if (!pagina->en_reemplazo && pagina->uso == 0 && pagina->modificada == 0) {
            pagina->en_reemplazo = true;
            puntero_clock = (idx + 1) % cantidad_marcos;

            pthread_mutex_unlock(&mutex_bitmap);
            pthread_mutex_unlock(&mutex_memoria);
            pthread_mutex_unlock(&mutex_tablas);
            pthread_setcancelstate(_old_cancel_state, NULL);
            return pagina; // (0,0) elegida en la primera pasada
        }
    }

    // =========================================================
    // PASADA 2 (P2): seleccionar la primera U=0 && M=1
    //   Reglas:
    //   - Evaluar marco
    //     * U=0 & M=1 -> seleccionar inmediatamente
    //     * U=0 & M=0 -> continuar (no candidata en P2)
    //     * U=1       -> NO candidata; recién AHORA bajar U=0 y continuar
    // =========================================================
    for (int i = 0; i < cantidad_marcos; i++) {
        const int idx = (start + i) % cantidad_marcos;
        if (!bitarray_test_bit(bitmap_marcos, idx)) {
            log_debug_seguro(worker_logger, "CLOCK-M [P2]: marco %d -> libre", idx);
            continue;
        }
        t_pagina* pagina = buscar_pagina_por_marco_unsafe(idx);
        if (!pagina) {
            log_debug_seguro(worker_logger, "CLOCK-M [P2]: marco %d -> bitmap=1 sin pagina", idx);
            continue;
        }
        log_debug_seguro(worker_logger,
            "CLOCK-M [P2]: marco %d -> pag %d | U=%d M=%d en_rep=%d",
            idx, pagina->numero_pagina, pagina->uso, pagina->modificada, pagina->en_reemplazo);

        if (pagina->en_reemplazo) continue;

        // 1) EVALUAR
        if (pagina->uso == 0) {
            if (pagina->modificada == 1) {
                // Candidata de P2
                pagina->en_reemplazo = true;
                puntero_clock = (idx + 1) % cantidad_marcos;

                pthread_mutex_unlock(&mutex_bitmap);
                pthread_mutex_unlock(&mutex_memoria);
                pthread_mutex_unlock(&mutex_tablas);
                pthread_setcancelstate(_old_cancel_state, NULL);
                return pagina; // (0,1) elegida en la segunda pasada
            } else {
                // (U=0, M=0) -> no candidata en P2, continuar SIN tocar U
                continue;
            }
        }

        // 2) BAJAR U DESPUÉS DE EVALUAR si no fue candidata (U=1 -> U=0 y seguir)
        pagina->uso = 0;
        log_debug_seguro(worker_logger, "CLOCK-M [P2]: marco %d -> bajé U; ahora U=0 M=%d",
                         idx, pagina->modificada);
        // continuar
    }

    // =========================================================
    // PASADA 3 (P3): igual a P1 (U=0 && M=0)
    // =========================================================
    for (int i = 0; i < cantidad_marcos; i++) {
        const int idx = (start + i) % cantidad_marcos;
        if (!bitarray_test_bit(bitmap_marcos, idx)) {
            log_debug_seguro(worker_logger, "CLOCK-M [P3]: marco %d -> libre", idx);
            continue;
        }
        t_pagina* pagina = buscar_pagina_por_marco_unsafe(idx);
        if (!pagina) {
            log_debug_seguro(worker_logger, "CLOCK-M [P3]: marco %d -> bitmap=1 sin pagina", idx);
            continue;
        }
        log_debug_seguro(worker_logger,
            "CLOCK-M [P3]: marco %d -> pag %d | U=%d M=%d en_rep=%d",
            idx, pagina->numero_pagina, pagina->uso, pagina->modificada, pagina->en_reemplazo);

        if (!pagina->en_reemplazo && pagina->uso == 0 && pagina->modificada == 0) {
            pagina->en_reemplazo = true;
            puntero_clock = (idx + 1) % cantidad_marcos;

            pthread_mutex_unlock(&mutex_bitmap);
            pthread_mutex_unlock(&mutex_memoria);
            pthread_mutex_unlock(&mutex_tablas);
            pthread_setcancelstate(_old_cancel_state, NULL);
            return pagina; // (0,0) elegida en la tercera pasada
        }
    }

    // =========================================================
    // PASADA 4 (P4): igual a P2 (U=0 && M=1; bajar U si U=1 después de evaluar)
    // =========================================================
    for (int i = 0; i < cantidad_marcos; i++) {
        const int idx = (start + i) % cantidad_marcos;
        if (!bitarray_test_bit(bitmap_marcos, idx)) {
            log_debug_seguro(worker_logger, "CLOCK-M [P4]: marco %d -> libre", idx);
            continue;
        }
        t_pagina* pagina = buscar_pagina_por_marco_unsafe(idx);
        if (!pagina) {
            log_debug_seguro(worker_logger, "CLOCK-M [P4]: marco %d -> bitmap=1 sin pagina", idx);
            continue;
        }
        log_debug_seguro(worker_logger,
            "CLOCK-M [P4]: marco %d -> pag %d | U=%d M=%d en_rep=%d",
            idx, pagina->numero_pagina, pagina->uso, pagina->modificada, pagina->en_reemplazo);

        if (pagina->en_reemplazo) continue;

        // 1) EVALUAR
        if (pagina->uso == 0) {
            if (pagina->modificada == 1) {
                pagina->en_reemplazo = true;
                puntero_clock = (idx + 1) % cantidad_marcos;

                pthread_mutex_unlock(&mutex_bitmap);
                pthread_mutex_unlock(&mutex_memoria);
                pthread_mutex_unlock(&mutex_tablas);
                pthread_setcancelstate(_old_cancel_state, NULL);
                return pagina; // (0,1) elegida en la cuarta pasada
            } else {
                // (U=0, M=0) -> no candidata en P4
                continue;
            }
        }

        // 2) BAJAR U DESPUÉS DE EVALUAR si no fue candidata
        pagina->uso = 0;
        log_debug_seguro(worker_logger, "CLOCK-M [P4]: marco %d -> bajé U; ahora U=0 M=%d",
                         idx, pagina->modificada);
        // continuar
    }

    // =========================================================
    // Sin candidata: diagnóstico y salida segura
    // =========================================================
    pthread_mutex_unlock(&mutex_bitmap);
    pthread_mutex_unlock(&mutex_memoria);
    pthread_mutex_unlock(&mutex_tablas);
    pthread_setcancelstate(_old_cancel_state, NULL);

    log_error_seguro(worker_logger, "CLOCK-M: no se encontró víctima. puntero_clock=%d", puntero_clock);

    dump_memoria();
    verificar_consistencia_bitmap_tablas();

    pthread_mutex_lock(&mutex_bitmap);
    for (int j = 0; j < cantidad_marcos; j++) {
        log_debug_seguro(worker_logger, "bitmap[%d]=%d", j,
                         bitarray_test_bit(bitmap_marcos, j) ? 1 : 0);
    }
    pthread_mutex_unlock(&mutex_bitmap);

    return NULL;
}




t_pagina* seleccionar_por_lru() {
    t_pagina* victima = NULL;
    uint64_t menor_timestamp = UINT64_MAX;

    void buscar_en_tabla(char* key, void* value) {
        t_list* tabla = (t_list*)value;
        for (int i = 0; i < list_size(tabla); i++) {
            t_pagina* pagina = list_get(tabla, i);
            if (!pagina->presente) continue;

            if (pagina->timestamp < menor_timestamp) {
                log_debug_seguro(worker_logger,"se comparan timestamp de paginas: pagina (%d) timestamp(%d), antiguo menor pagina(%d) timestamp(%d)", pagina->numero_pagina, pagina->timestamp, victima ? victima->numero_pagina: -1,menor_timestamp);
                menor_timestamp = pagina->timestamp;
                victima = pagina;
            }
        }
    }

    safe_dictionary_iterator(tablas_de_paginas, buscar_en_tabla);
    return victima;
}