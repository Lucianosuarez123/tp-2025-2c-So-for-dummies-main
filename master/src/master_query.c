#include <../includes/master_query.h>

void* atender_master_query(void* arg){
    t_query* query_conectado = (t_query*) arg;
	int id_query = query_conectado->id_query;
	int socket = query_conectado->socket;
	bool salida = 1;
	while (salida) { // Ahora espera reconexiones
        int cod_op = recibir_operacion(socket);
		switch (cod_op) {
		// case <insertar cod_op>:
        //     funcion_que_utiliza_buffer_del_mensaje(query_conectado);
		// break;
		case DESCONEXION:
			log_debug_seguro(master_logger,"se desconecto query. id query: %d",id_query);
			manejar_desconexion_query(id_query,socket);
			salida=0;
			break;
		default:
			//log_warning(master_logger,"Operacion desconocida. No quieras meter la pata");
			break;
		}
	}

	return NULL;
}


void manejar_desconexion_query(int id_query, int socket){
	log_debug_seguro(master_logger,"inicianco manejo de desconexion de query:%d", id_query);
	pthread_mutex_lock(&mutex_ready);
	pthread_mutex_lock(&mutex_lista_workers);
	pthread_mutex_lock(&mutex_exec);
	t_query* query = buscar_query_en_cola_por_id(id_query, cola_ready);
	bool fue_encontrada_en_cola_ready=true;
	if (query==NULL){
		log_debug_seguro(master_logger, "no se encontro query desconectada en cola READY, se procede a buscarla en cola EXEC");
		fue_encontrada_en_cola_ready=false;
		query = buscar_query_en_cola_por_id(id_query, cola_exec);
		if(query==NULL){
			log_debug_seguro(master_logger,"no se encontro la query en ninguna cola. ya debe haber finalizado");
			pthread_mutex_unlock(&mutex_exec);
			pthread_mutex_unlock(&mutex_lista_workers);
			pthread_mutex_unlock(&mutex_ready);
			return;
		}
	}
	if (fue_encontrada_en_cola_ready){
		pthread_mutex_lock(&query->mutex_query);
		query->finalizo=true;
		int prioridad=query->prioridad;
		pthread_mutex_unlock(&query->mutex_query);
		log_debug_seguro(master_logger,"ahora manejamos desconexion sabiendo que la query esta en READY");
		mover_query_seguro(query,READY, EXIT);
		log_info_seguro(master_logger,"## Se desconecta un Query Control. Se finaliza la Query <%d> con prioridad <%d>. Nivel multiprocesamiento <%d>", id_query, prioridad, list_size(lista_workers));
		destruir_query(query);
		pthread_mutex_unlock(&mutex_exec);
		pthread_mutex_unlock(&mutex_lista_workers);
		pthread_mutex_unlock(&mutex_ready);
		return;
	}

	log_debug_seguro(master_logger,"ahora manejamos desconexion sabiendo que la query esta en EXEC");
	t_worker* worker = buscar_worker_por_query_id(id_query);
	if (worker==NULL){
		log_error_seguro(master_logger,"no se encontro worker ejecutando a la query %d", id_query);
		pthread_mutex_unlock(&mutex_exec);
		pthread_mutex_unlock(&mutex_lista_workers);
		pthread_mutex_unlock(&mutex_ready);
		return;
	}
	pthread_mutex_lock(&query->mutex_query);
	query->finalizo=true;
	pthread_mutex_unlock(&query->mutex_query);
	enviar_interrupcion_a_worker(worker->socket, id_query);
	pthread_mutex_unlock(&mutex_exec);
	pthread_mutex_unlock(&mutex_lista_workers);
	pthread_mutex_unlock(&mutex_ready);
}

void enviar_finalizacion_query_a_query_control(int socket, char* motivo){
	t_buffer* buffer= crear_buffer();
	if(buffer==NULL){
		log_error_seguro(master_logger,"no se pudo crear buffer para enviar finalizacion de la query");
		return;
	}
	agregar_string_a_buffer(buffer, motivo);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(FINALIZACION_DE_QUERY, buffer);
	enviar_paquete(paquete, socket);
	liberar_paquete(paquete);
}

void enviar_lectura_a_query_control(int socket,int query_id, char* id_worker, char* file_y_tag, char* contenido){
	t_buffer* buffer = crear_buffer();
	agregar_string_a_buffer(buffer, file_y_tag);
	agregar_string_a_buffer(buffer, contenido);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(LECTURA_REALIZADA,buffer);
	enviar_paquete(paquete, socket);
	log_info_seguro(master_logger, "## Se envía un mensaje de lectura de la Query <%d> en el Worker <%s> al Query Control", query_id, id_worker);
	log_debug_seguro(master_logger,"contenido de la lectura:%s", contenido);
	liberar_paquete(paquete);
}