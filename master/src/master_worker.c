#include <../includes/master_worker.h>

void* atender_master_worker(void* arg){
    t_worker* worker_conectado = (t_worker*) arg;
    char* id_worker = worker_conectado->id_worker;
    int socket_worker= worker_conectado->socket;
	bool salida = 1;
	while (salida) { // Ahora espera reconexiones
        int cod_op = recibir_operacion(socket_worker);
		switch (cod_op) {
		// case <insertar cod_op>:
        //     funcion_que_utiliza_buffer_del_mensaje(worker_conectado);
		// break;
        case RESPUESTA_INTERRUPCION:
            recibir_respuesta_interrupcion(socket_worker, id_worker);
            break;
        case OP_END:
            recibir_finalizacion_de_query(socket_worker, id_worker);
            break;
        case OP_LECTURA_REALIZADA:
            recibir_lectura_realizada(socket_worker, id_worker);
            break;
		case DESCONEXION:
			log_debug_seguro(master_logger,"se desconecto worker. id worker: %s",id_worker);
            manejar_desconexion_worker(id_worker, socket_worker);
			salida=0;
			break;
		default:
			log_warning(master_logger,"Operacion desconocida. No quieras meter la pata");
			break;
		}
	}

	return NULL;
}

void enviar_query_a_worker(t_worker* worker, t_query* query) {
    t_buffer* buffer = crear_buffer();
    agregar_int_a_buffer(buffer, query->id_query);
    agregar_int_a_buffer(buffer, query->program_counter);
    agregar_string_a_buffer(buffer, query->archivo_query);

    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(EJECUTAR_QUERY, buffer);
    enviar_paquete(paquete, worker->socket);
    liberar_paquete(paquete);

    // Marcar al Worker como ocupado
    worker->ocupado = true;
    worker->query_asignada = query;

    log_info_seguro(master_logger,
        "## Se envía la Query %d (%d) al Worker %s", query->id_query, query->prioridad, worker->id_worker);
}

void enviar_interrupcion_a_worker(int socket, int id_query){
    t_buffer* buffer = crear_buffer();
    agregar_int_a_buffer(buffer, id_query);
    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(INTERRUPCION, buffer);
    enviar_paquete(paquete, socket);
    liberar_paquete(paquete);
    log_debug_seguro(master_logger, "se envio interrupcion a worker");
    return;
}

void recibir_respuesta_interrupcion(int socket, char* id_worker){
    t_buffer* buffer = recibir_buffer(socket);
    int query_id = extraer_int(buffer);
    int pc_actualizado = extraer_int(buffer);
    liberar_buffer(buffer);
    pthread_mutex_lock(&mutex_ready);
    pthread_mutex_lock(&mutex_lista_workers);
    pthread_mutex_lock(&mutex_exec);

    t_worker* worker = buscar_worker_por_query_id(query_id);
    if(worker==NULL || strcmp(worker->id_worker, id_worker) != 0){
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        pthread_mutex_unlock(&mutex_ready);
        log_warning_seguro(master_logger,"llego una respuesta de interrupcion pero no se encontro el worker buscado");
        return;
    }
    if(worker->query_asignada==NULL){
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        pthread_mutex_unlock(&mutex_ready);
        log_debug_seguro(master_logger,"llego una respuesta de interrupcion pero no esta ejecutando una query actualmente");
        return;
    }
    if(worker->query_asignada->estado_actual!= EXEC){
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        pthread_mutex_unlock(&mutex_ready);
        log_warning_seguro(master_logger,"llego una respuesta de interrupcion pero la query recibida no esta en exec");
        return;
    }

    EstadoQuery siguiente_estado=READY;

    pthread_mutex_lock(&worker->query_asignada->mutex_query);
    worker->query_asignada->program_counter=pc_actualizado;
    if(worker->query_asignada->finalizo){
        siguiente_estado=EXIT;
    }
    pthread_mutex_unlock(&worker->query_asignada->mutex_query);

    mover_query_seguro(worker->query_asignada, EXEC, siguiente_estado);

    pthread_mutex_lock(&worker->query_asignada->mutex_query);
    worker->query_asignada->esperando_desalojo=false;
    pthread_mutex_unlock(&worker->query_asignada->mutex_query);

    log_info_seguro(master_logger,"## Se desaloja la Query <%d> (%d) del Worker <%s> - Motivo: %s", query_id, worker->query_asignada->prioridad, worker->id_worker, (siguiente_estado==READY) ? "PRIORIDAD" : "DESCONEXION");
    t_query* query = worker->query_asignada;
    worker->query_asignada=NULL;
    worker->ocupado=false;

    if(siguiente_estado==EXIT){
        log_info_seguro(master_logger,"## Se desconecta un Query Control. Se finaliza la Query <%d> con prioridad <%d>. Nivel multiprocesamiento <%d>", query->id_query, query->prioridad, list_size(lista_workers));
        destruir_query(query);
    }
    pthread_mutex_unlock(&mutex_exec);
    pthread_mutex_unlock(&mutex_lista_workers);
    pthread_mutex_unlock(&mutex_ready);
    sem_post(&sem_worker_libre);
    sem_post(&sem_eventos);
}

void manejar_desconexion_worker(char* id_worker, int socket){
    pthread_mutex_lock(&mutex_lista_workers);
    pthread_mutex_lock(&mutex_exec);
    t_worker* worker = buscar_worker_por_id(id_worker);
    if (worker==NULL){
        log_error_seguro(master_logger,"no se encontro el worker buscado por su id:%s para la desconexion", id_worker);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        return;
    }
    if(worker->query_asignada==NULL){
        log_debug_seguro(master_logger,"el worker:%s se desconecta sin tener asignada ninguna query", id_worker);
        list_remove_element(lista_workers, worker);
        destruir_worker(worker);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        return;
    }
    mover_query_seguro(worker->query_asignada, EXEC, EXIT);
    pthread_mutex_lock(&worker->query_asignada->mutex_query);
    int id_query = worker->query_asignada->id_query;
    int socket_query = worker->query_asignada->socket;
    pthread_mutex_unlock(&worker->query_asignada->mutex_query);
    destruir_query(worker->query_asignada);
    worker->query_asignada=NULL;
    if (!list_remove_element(lista_workers, worker)){
        log_error_seguro(master_logger,"no se pudo sacar de la lista de workers al worker:%s", id_worker);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        return;
    }
    log_info_seguro(master_logger, "## Se desconecta el Worker <%s> - Se finaliza la Query <%d> - Cantidad total de Workers: <%d>", id_worker, id_query, list_size(lista_workers));
    enviar_finalizacion_query_a_query_control(socket_query, "DESCONEXION DE WORKER");
    destruir_worker(worker);
    pthread_mutex_unlock(&mutex_exec);
    pthread_mutex_unlock(&mutex_lista_workers);
}


void recibir_finalizacion_de_query(int socket_worker,char* id_worker){
    t_buffer* buffer = recibir_buffer(socket_worker);
    int id_query = extraer_int(buffer);
    char* motivo = extraer_string(buffer);
   
    pthread_mutex_lock(&mutex_lista_workers);
    pthread_mutex_lock(&mutex_exec);
    t_worker* worker = buscar_worker_por_id(id_worker);
    log_debug_seguro(master_logger,"termine de buscar el worker por id");
    if (worker==NULL){
        log_error_seguro(master_logger,"no se encontro el worker buscado por su id:%s para la finalizacion de query", id_worker);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        liberar_buffer(buffer);
        return;
    }
    if(worker->query_asignada==NULL){
        log_debug_seguro(master_logger,"el worker:%s no tiene query asignada", id_worker);
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        liberar_buffer(buffer);
        return;
    }
    if(id_query != worker->query_asignada->id_query){
        log_error_seguro(master_logger,"la id de la query a finalizar y la id de la query que el worker tiene asignada no coinciden");
        pthread_mutex_unlock(&mutex_exec);
        pthread_mutex_unlock(&mutex_lista_workers);
        liberar_buffer(buffer);
        return;
    }
    mover_query_seguro(worker->query_asignada, EXEC, EXIT);
    pthread_mutex_lock(&worker->query_asignada->mutex_query);
    int socket_query = worker->query_asignada->socket;
    pthread_mutex_unlock(&worker->query_asignada->mutex_query);
    destruir_query(worker->query_asignada);
    enviar_finalizacion_query_a_query_control(socket_query, motivo);
    log_info_seguro(master_logger,"## Se terminó la Query <%d> en el Worker <%s>",id_query, worker->id_worker);
    free(motivo);
    worker->query_asignada=NULL;
    worker->ocupado=false;
    pthread_mutex_unlock(&mutex_exec);
    pthread_mutex_unlock(&mutex_lista_workers);
    sem_post(&sem_worker_libre);
    sem_post(&sem_eventos);
    liberar_buffer(buffer);
}

void  recibir_lectura_realizada(int socket_worker, char* id_worker){
    t_buffer* buffer = recibir_buffer(socket_worker);
    int query_id = extraer_int(buffer);
    char* file_y_tag = extraer_string(buffer);
    char* contenido_leido = extraer_string(buffer);
    pthread_mutex_lock(&mutex_lista_workers);
    t_worker* worker = buscar_worker_por_query_id(query_id);
    if(worker==NULL){
        log_error_seguro(master_logger, "no se encontro worker que tenga asignada la query:%d enviada para la lectura recibida", query_id);
        pthread_mutex_unlock(&mutex_lista_workers);
        liberar_buffer(buffer);
        return;
    }
    enviar_lectura_a_query_control(worker->query_asignada->socket, query_id, worker->id_worker, file_y_tag,contenido_leido);

    free(file_y_tag);
    free(contenido_leido);

    pthread_mutex_unlock(&mutex_lista_workers);
    liberar_buffer(buffer);
    return;
}