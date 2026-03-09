#include "../includes/planificador.h"

 void* planificador(void* _) {return NULL;}
//     while (1) {
//         sem_wait(&sem_queries_en_ready);

//         pthread_mutex_lock(&mutex_ready);
//         if (list_is_empty(cola_ready)) {
//             pthread_mutex_unlock(&mutex_ready);
//             continue;
//         }

//         if (strcmp(ALGORITMO_PLANIFICACION, "PRIORIDADES") == 0) {
//             ordenar_cola_ready(); // Aplica aging
//         }

//         t_query* query = list_get(cola_ready, 0);
//         pthread_mutex_lock(&mutex_lista_workers);
//         t_worker* worker_libre = obtener_worker_libre(); // sin mutex interno
//         log_debug_seguro(master_logger,"verifico si hay worker libre");
//         if (worker_libre != NULL) {
//             pthread_mutex_lock(&mutex_exec);
//             mover_query_seguro(query, READY, EXEC);
//             enviar_query_a_worker(worker_libre, query);
//             worker_libre->ocupado = true;
//             worker_libre->query_asignada = query;
//             pthread_mutex_unlock(&mutex_exec);
//             pthread_mutex_unlock(&mutex_lista_workers);
//             pthread_mutex_unlock(&mutex_ready);
//             continue;
//         }
//         log_debug_seguro(master_logger,"no hay worker libre");
//         // No hay Workers libres y estamos en PRIORIDADES
//         if (strcmp(ALGORITMO_PLANIFICACION, "PRIORIDADES") == 0) {
//             pthread_mutex_lock(&mutex_exec);
//             if (!list_is_empty(cola_exec)) {
//                 t_query* query_menor_prioridad = list_get_maximum(cola_exec, query_menor_prioridad_que_otra);

//                 pthread_mutex_lock(&query_menor_prioridad->mutex_query);
//                 bool puede_desalojar = query_menor_prioridad &&
//                                        !query_menor_prioridad->esperando_desalojo &&
//                                        query->id_query != query_menor_prioridad->id_query &&
//                                        comparar_query_por_prioridad_con_aging(query, query_menor_prioridad);

//                 if (puede_desalojar) {
//                     t_worker* worker_menos_prioridad = buscar_worker_por_query_id(query_menor_prioridad->id_query);
//                     if (worker_menos_prioridad) {
//                         enviar_interrupcion_a_worker(worker_menos_prioridad->socket, query_menor_prioridad->id_query);
//                         query_menor_prioridad->esperando_desalojo = true;
//                         log_info_seguro(master_logger, "## (%d) - Desalojado por prioridad", query_menor_prioridad->id_query);
//                     }
//                 }
//                 pthread_mutex_unlock(&query_menor_prioridad->mutex_query);
//             }
//             pthread_mutex_unlock(&mutex_exec);
//             pthread_mutex_unlock(&mutex_lista_workers);
//             pthread_mutex_unlock(&mutex_ready);
//             // Esperar que se libere un Worker o se complete el desalojo
//             //sem_wait(&sem_espera_prioridades);
//             sem_wait(&sem_worker_libre);
//             sem_post(&sem_queries_en_ready);
//             continue;
//         }
//         log_debug_seguro(master_logger,"como no es prioridades, voy por aca");
//         // FIFO sin Worker libre
//         pthread_mutex_unlock(&mutex_lista_workers);
//         pthread_mutex_unlock(&mutex_ready);
//         log_debug_seguro(master_logger,"espero a que se postee un worker");
//         sem_wait(&sem_worker_libre);
//         log_debug_seguro(master_logger,"pase de ese post de worker libre");
//         sem_post(&sem_queries_en_ready);

//     }

//     return NULL;
// }

// void* planificador_con_desalojo(void* _){
//     while (1) {
//         sem_wait(&sem_queries_en_ready);

//         pthread_mutex_lock(&mutex_ready);
//         if (list_is_empty(cola_ready)) {
//             pthread_mutex_unlock(&mutex_ready);
//             continue;
//         }
//         ordenar_cola_ready();
//         t_query* query = list_get(cola_ready, 0);
//         pthread_mutex_lock(&mutex_lista_workers);
//         t_worker* worker_libre = obtener_worker_libre();
//         log_debug_seguro(master_logger,"verifico si hay worker libre");
//         if (worker_libre != NULL) {
//             pthread_mutex_lock(&mutex_exec);
//             mover_query_seguro(query, READY, EXEC);
//             enviar_query_a_worker(worker_libre, query);
//             worker_libre->ocupado = true;
//             worker_libre->query_asignada = query;
//             pthread_mutex_unlock(&mutex_exec);
//             pthread_mutex_unlock(&mutex_lista_workers);
//             pthread_mutex_unlock(&mutex_ready);
//             continue;
//         }
//         log_debug_seguro(master_logger,"no hay worker libre");
//         pthread_mutex_lock(&mutex_exec);
//         if (!list_is_empty(cola_exec)) {
//             t_query* query_menor_prioridad = list_get_maximum(cola_exec, query_menor_prioridad_que_otra);
//             pthread_mutex_lock(&query_menor_prioridad->mutex_query);
//             bool puede_desalojar = query_menor_prioridad && !query_menor_prioridad->esperando_desalojo && query->id_query != query_menor_prioridad->id_query && comparar_query_por_prioridad_con_aging(query, query_menor_prioridad);

//             if (puede_desalojar) {
//                 t_worker* worker_menos_prioridad = buscar_worker_por_query_id(query_menor_prioridad->id_query);
//                 if (worker_menos_prioridad) {
//                     enviar_interrupcion_a_worker(worker_menos_prioridad->socket, query_menor_prioridad->id_query);
//                     query_menor_prioridad->esperando_desalojo = true;
//                     log_info_seguro(master_logger, "## (%d) - Desalojado por prioridad", query_menor_prioridad->id_query);
//                     pthread_mutex_unlock(&query_menor_prioridad->mutex_query);
//                     pthread_mutex_unlock(&mutex_exec);
//                     pthread_mutex_unlock(&mutex_lista_workers);
//                     pthread_mutex_unlock(&mutex_ready);
//                     // Esperar que se libere un Worker o se complete el desalojo
//                     sem_wait(&sem_worker_libre);
//                     sem_post(&sem_queries_en_ready);
//                     continue;
//                 }
//             }
//             pthread_mutex_unlock(&query_menor_prioridad->mutex_query);
//             pthread_mutex_unlock(&mutex_exec);
//             pthread_mutex_unlock(&mutex_lista_workers);
//             pthread_mutex_unlock(&mutex_ready);
//             sem_wait(&sem_espera_prioridades);
//             continue;
//         }
//     }
// }

#include <errno.h>
#include <time.h>
#include <stdint.h>

// Helper para normalizar timespec (por si sumamos nanosegundos)
static inline void normalize_timespec(struct timespec* ts) {
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += ts->tv_nsec / 1000000000L;
        ts->tv_nsec = ts->tv_nsec % 1000000000L;
    } else if (ts->tv_nsec < 0) {
        long sec = (-(ts->tv_nsec) + 999999999L) / 1000000000L;
        ts->tv_sec -= sec;
        ts->tv_nsec += sec * 1000000000L;
    }
}

void* planificador_con_desalojo(void* _) {
    while (1) {
        //Calcular si existe próximo vencimiento de aging
        pthread_mutex_lock(&mutex_ready);
        t_query* q_aging = proxima_query_a_envejecer(); // NULL si cola_ready vacía
        pthread_mutex_unlock(&mutex_ready);

        int res;
        if (q_aging == NULL) {
            // No hay queries en READY → dormir hasta algún evento externo
            // (nueva query, worker libre, desconexión, etc.)
            res = sem_wait(&sem_eventos);
        } else {
            // Hay una candidata a envejecer → calcular timeout absoluto
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);

            int64_t restante = TIEMPO_AGING - (get_time_ms() - q_aging->ultimo_aging);
            if (restante < 0) restante = 0;

            ts.tv_sec  += restante / 1000;
            ts.tv_nsec += (restante % 1000) * 1000000L;
            normalize_timespec(&ts);

            // Esperamos hasta evento (post) o hasta que venza el próximo aging
            res = sem_timedwait(&sem_eventos, &ts);
        }


        if (res == -1 && errno == ETIMEDOUT) {
            bool cambio_prioridad=false;
            pthread_mutex_lock(&mutex_ready);
            if (q_aging) {
                pthread_mutex_lock(&q_aging->mutex_query);
                if (q_aging->estado_actual==READY && q_aging->prioridad > 0 ) {
                    int anterior = q_aging->prioridad;
                    q_aging->prioridad--;
                    q_aging->ultimo_aging = get_time_ms();
                    log_info_seguro(master_logger, "## %d Cambio de prioridad: < %d > - < %d >",
                                    q_aging->id_query, anterior, q_aging->prioridad);
                    cambio_prioridad=true;
                }
                pthread_mutex_unlock(&q_aging->mutex_query);

                if(cambio_prioridad) ordenar_cola_ready();
            }
            pthread_mutex_unlock(&mutex_ready);
            if(!cambio_prioridad) continue;
        }


        // Se despertó por EVENTO → intentar planificar
        pthread_mutex_lock(&mutex_ready);
        if (list_is_empty(cola_ready)) {
            // No hay nada para ejecutar; volvemos a esperar próximos eventos/aging
            pthread_mutex_unlock(&mutex_ready);
            continue;
        }
        ordenar_cola_ready();
        t_query* query = list_get(cola_ready, 0);
        pthread_mutex_lock(&mutex_lista_workers);
        t_worker* worker_libre = obtener_worker_libre();
        log_debug_seguro(master_logger, "verifico si hay worker libre");

        if (worker_libre != NULL) {
            pthread_mutex_lock(&mutex_exec);
            mover_query_seguro(query, READY, EXEC);
            enviar_query_a_worker(worker_libre, query);
            worker_libre->ocupado = true;
            worker_libre->query_asignada = query;
            pthread_mutex_unlock(&mutex_exec);
            pthread_mutex_unlock(&mutex_lista_workers);
            pthread_mutex_unlock(&mutex_ready);
            // Siguiente iteración: recalcular aging o esperar eventos
            continue;
        }

        log_debug_seguro(master_logger, "no hay worker libre");
        pthread_mutex_lock(&mutex_exec);
        if (!list_is_empty(cola_exec)) {
            t_query* query_menor_prioridad =
                list_get_maximum(cola_exec, query_menor_prioridad_que_otra);

            pthread_mutex_lock(&query_menor_prioridad->mutex_query);

            bool puede_desalojar = query_menor_prioridad &&
                                   !query_menor_prioridad->esperando_desalojo &&
                                   query->id_query != query_menor_prioridad->id_query &&
                                   comparar_query_por_prioridad_con_aging(query, query_menor_prioridad);

            if (puede_desalojar) {
                t_worker* worker_menos_prioridad =
                    buscar_worker_por_query_id(query_menor_prioridad->id_query);
                if (worker_menos_prioridad) {
                    enviar_interrupcion_a_worker(worker_menos_prioridad->socket,
                                                 query_menor_prioridad->id_query);
                    query_menor_prioridad->esperando_desalojo = true;
                    log_debug_seguro(master_logger,
                                    "## (%d) - Desalojado por prioridad",
                                    query_menor_prioridad->id_query);

                    pthread_mutex_unlock(&query_menor_prioridad->mutex_query);
                    pthread_mutex_unlock(&mutex_exec);
                    pthread_mutex_unlock(&mutex_lista_workers);
                    pthread_mutex_unlock(&mutex_ready);

                    continue;
                }
            }
            pthread_mutex_unlock(&query_menor_prioridad->mutex_query);
        }
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        pthread_mutex_unlock(&mutex_ready);
        // Vuelve al loop; sin workers y sin desalojo → calcular aging y/o esperar eventos
    }
}

void* planificador_fifo(void* _) {
    while (1) {
        sem_wait(&sem_queries_en_ready);

        pthread_mutex_lock(&mutex_ready);
        if (list_is_empty(cola_ready)) {
            pthread_mutex_unlock(&mutex_ready);
            continue;
        }


        t_query* query = list_get(cola_ready, 0);
        pthread_mutex_lock(&mutex_lista_workers);
        t_worker* worker_libre = obtener_worker_libre(); // sin mutex interno
        log_debug_seguro(master_logger,"verifico si hay worker libre");
        if (worker_libre != NULL) {
            pthread_mutex_lock(&mutex_exec);
            mover_query_seguro(query, READY, EXEC);
            enviar_query_a_worker(worker_libre, query);
            worker_libre->ocupado = true;
            worker_libre->query_asignada = query;
            pthread_mutex_unlock(&mutex_exec);
            pthread_mutex_unlock(&mutex_lista_workers);
            pthread_mutex_unlock(&mutex_ready);
            continue;
        }
        log_debug_seguro(master_logger,"no hay worker libre");
        log_debug_seguro(master_logger,"como no es prioridades, voy por aca");
        // FIFO sin Worker libre
        pthread_mutex_unlock(&mutex_lista_workers);
        pthread_mutex_unlock(&mutex_ready);
        log_debug_seguro(master_logger,"espero a que se postee un worker");
        sem_wait(&sem_worker_libre);
        log_debug_seguro(master_logger,"pase de ese post de worker libre");
        sem_post(&sem_queries_en_ready);
    }

    return NULL;
}

void* query_menor_prioridad_que_otra(void* a, void* b) {
    t_query* q1 = (t_query*) a;
    t_query* q2 = (t_query*) b;

    int prioridad1 = q1->prioridad;
    int prioridad2 = q2->prioridad;

   if(prioridad1 > prioridad2) return q1;
   else return q2;
}

