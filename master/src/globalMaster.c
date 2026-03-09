#include <../includes/globalMaster.h>


t_query* crear_query(int nuevo_cliente_fd, char* archivo_query, int prioridad) {
    t_query* nueva_query = malloc(sizeof(t_query));
    if (nueva_query == NULL) {
        log_error_seguro(master_logger, "Error al asignar memoria para nueva query.");
        close(nuevo_cliente_fd);
        pthread_exit(NULL);
    }

    // Asignar ID único
    pthread_mutex_lock(&mutex_id_query);
    nueva_query->id_query = id_query++;
    pthread_mutex_unlock(&mutex_id_query);

    // Inicializar campos
    nueva_query->socket = nuevo_cliente_fd;
    nueva_query->archivo_query = strdup(archivo_query); // strdup para evitar problemas
    nueva_query->prioridad = prioridad;
    nueva_query->program_counter = 0;
    nueva_query->estado_actual = READY;
    nueva_query->esperando_desalojo = false;
    nueva_query->finalizo = false;
    nueva_query->ultimo_aging = get_time_ms();
    pthread_mutex_init(&nueva_query->mutex_query, NULL);


    pthread_mutex_lock(&mutex_lista_querys);
    list_add(lista_querys, nueva_query);
    pthread_mutex_unlock(&mutex_lista_querys);

    // Avisar al planificador
    log_debug_seguro(master_logger, "## Se crea la Query %d con prioridad %d - Estado: READY", nueva_query->id_query, nueva_query->prioridad);
    // Encolar en READY
    pthread_mutex_lock(&mutex_ready);
    list_add(cola_ready, nueva_query);
    pthread_mutex_unlock(&mutex_ready);
    sem_post(&sem_queries_en_ready);
    sem_post(&sem_eventos);
    return nueva_query;
}


bool mover_query_seguro(t_query* query, EstadoQuery origen, EstadoQuery destino) {
    if (!query || origen == destino) {
        log_warning_seguro(master_logger, "estado de origen y destido identicos. estado:%s", estado_a_string(origen));
        return false;
    }

    t_list* cola_origen  = obtener_cola_por_estado(origen);
    t_list* cola_destino = obtener_cola_por_estado(destino);

    if (cola_origen)  list_remove_element(cola_origen, query);

    if (cola_destino) list_add(cola_destino, query);

    //Actualizar estado de la query
    pthread_mutex_lock(&query->mutex_query);
    if (query->estado_actual != origen) {
        log_warning_seguro(master_logger,
            "Estado actual de la query no coincide con el supuesto origen. ID: %d, Estado real: %d, Origen esperado: %d, Destino: %d",
            query->id_query, query->estado_actual, origen, destino);
        pthread_mutex_unlock(&query->mutex_query);
        return false;
    }

    query->estado_actual = destino;

    // Si entra en READY: reinicia aging y limpia flags de desalojo
    if (destino == READY) {
        query->ultimo_aging = get_time_ms();
        query->esperando_desalojo = false;

    }

    // Si se va a EXIT: aseguramos que no quede marcado esperando desalojo
    if (destino == EXIT) {
        if(origen== READY && strcmp(ALGORITMO_PLANIFICACION, "PRIORIDADES") == 0) sem_post(&sem_eventos);
        query->esperando_desalojo = false;
    }
    pthread_mutex_unlock(&query->mutex_query);

   //Notificar al planificador *solo* cuando entra a READY
    if (destino == READY) {
        if (strcmp(ALGORITMO_PLANIFICACION, "PRIORIDADES") == 0) {
            sem_post(&sem_eventos);
        } else {
            sem_post(&sem_queries_en_ready);
        }
    }
    log_debug_seguro(master_logger, "## Query %d pasa de %s a %s", query->id_query, estado_a_string(origen), estado_a_string(destino));
    return true;
}

t_worker* obtener_worker_libre() {

    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* worker = list_get(lista_workers, i);
        if (!worker->ocupado) {
            return worker;
        }
    }

    return NULL;
}

// Obtener el nombre del estado como string
const char* estado_a_string(EstadoQuery estado) {
    switch (estado) {
        case READY: return "READY";
        case EXEC: return "EXEC";
        case EXIT: return "EXIT";
        default: return "DESCONOCIDO";
    }
}
// Obtener cola por estado
t_list* obtener_cola_por_estado(EstadoQuery estado) {
    switch (estado) {
        case READY: return cola_ready;
        case EXEC: return cola_exec;
        default: return NULL;
    }

}
bool comparar_query_por_prioridad_con_aging_para_ordenar(void* a, void* b) {
    t_query* q1 = (t_query*) a;
    t_query* q2 = (t_query*) b;

    int prioridad1 = q1->prioridad;
    int prioridad2 = q2->prioridad;
    bool resultado = prioridad1 < prioridad2;


    return resultado;
}

bool comparar_query_por_prioridad_con_aging(void* a, void* b) {
    t_query* q1 = (t_query*) a;
    t_query* q2 = (t_query*) b;

    int prioridad1 = q1->prioridad;
    int prioridad2 = q2->prioridad;
    bool resultado = prioridad1 < prioridad2;

    return resultado;
}
void ordenar_cola_ready() {
    if(strcmp(ALGORITMO_PLANIFICACION, "PRIORIDADES") == 0) list_sort(cola_ready, comparar_query_por_prioridad_con_aging_para_ordenar);
}


t_worker* buscar_worker_por_query_id(int id_query) {
    bool buscar_worker(void* ptr) {
        t_worker* worker = (t_worker*) ptr;
        if (worker == NULL || worker->query_asignada == NULL) {
            return false;
        }
        return worker->query_asignada->id_query == id_query;
    }

    return list_find(lista_workers, buscar_worker);
}



t_worker* buscar_worker_por_id(char* id_worker){
     bool buscar_worker(void* ptr) {
        t_worker* worker = (t_worker*) ptr;
        return strcmp(worker->id_worker, id_worker)==0;
    }

    t_worker* worker_encontrado = list_find(lista_workers, buscar_worker);

    if (worker_encontrado != NULL) {
        return worker_encontrado;
    }
    return NULL;
}

t_query* buscar_query_en_cola_por_id(int id_query, t_list* cola){
    bool buscar_query(void* ptr) {
        t_query* query = (t_query*) ptr;
        return id_query==query->id_query;
    }

    t_query* query_encontrado = list_find(cola, buscar_query);

    if (query_encontrado != NULL) {
        return query_encontrado;
    }
    return NULL;
}

void destruir_query(t_query* query) {
    if (query == NULL) return;
    pthread_mutex_lock(&query->mutex_query);
    free(query->archivo_query);


    pthread_mutex_unlock(&query->mutex_query);
    pthread_mutex_destroy(&query->mutex_query);


    free(query);
}
void destruir_worker(t_worker* worker) {
    if (worker == NULL) return;

    free(worker->id_worker);

    if (worker->query_asignada != NULL) {
        worker->query_asignada=NULL;

    }

    free(worker);
}

uint64_t get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;
}

t_query* proxima_query_a_envejecer(void) {
    if (list_is_empty(cola_ready)) return NULL;

    // Si está configurado sin aging, no hay candidata.
    if (TIEMPO_AGING <= 0) return NULL;

    t_query* candidata = NULL;
    int64_t menor_restante = INT64_MAX;


    uint64_t ahora = get_time_ms();

    // Precondición: el caller tiene mutex_ready tomado.
    for (int i = 0; i < list_size(cola_ready); i++) {
        t_query* q = list_get(cola_ready, i);
        if (q == NULL) continue;


        pthread_mutex_lock(&q->mutex_query);
        bool elegible = (q->estado_actual == READY) && (!q->finalizo) && (q->prioridad > 0);
        uint64_t last = q->ultimo_aging;
        pthread_mutex_unlock(&q->mutex_query);

        if (!elegible) continue;

        int64_t transcurrido = (int64_t)(ahora - last);
        int64_t restante = (int64_t)TIEMPO_AGING - transcurrido;
        if (restante < 0) restante = 0;

        if (restante < menor_restante) {
            menor_restante = restante;
            candidata = q;
        }
    }

    // Puede ser NULL si:
    // - No hay READY,
    // - Todas tienen prioridad 0,
    // - Están finalizadas,
    // - TIEMPO_AGING == 0 (sin aging).
    return candidata;
}

