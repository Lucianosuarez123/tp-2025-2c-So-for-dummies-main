#include <../includes/storage_worker.h>

//TODO: Ver si en vez de enviar solo un respuesta ok, enviar una respuesta hacia el worker la cual informe si se realizo con exito o no la operacion solicitada, por ejemplo enviando en el buffer un 1 o 0
//* DETALLE : las operaciones para agregar al buffer son agregar_int() o agregar_string(), y para extraer luego de recibir el buffer son extraer_int() y extraer_string()

void* atender_storage_worker(void* arg){
    t_worker* worker_conectado = (t_worker*) arg;
	bool salida = 1;
	while (salida) { // Ahora espera reconexiones
		log_debug_seguro(storage_logger,"esperando operaciones del worker %s", worker_conectado);
        int cod_op = recibir_operacion(worker_conectado->socket);
		log_debug_seguro(storage_logger,"cod op recibido de worker %s, cod op:%d", worker_conectado->id_worker, cod_op);
		switch (cod_op) {
		case OP_CREATE:
			responder_op_create(worker_conectado);
			break;
		case OP_TRUNCATE:
			responder_op_truncate(worker_conectado);
			break;
		case OP_WRITE:
			responder_op_write(worker_conectado);
			break;
		case OP_TAG:
			responder_op_tag(worker_conectado);
			break;
		case OP_COMMIT:
			responder_op_commit(worker_conectado);
			break;
		case OP_DELETE:
			responder_op_delete(worker_conectado);
			break;
		case OP_PEDIR_PAGINA:
			responder_op_pedir_pagina(worker_conectado);
			break;
		case DESCONEXION:
			log_debug_seguro(storage_logger,"se desconecto worker. id worker: %s",worker_conectado->id_worker);
            manejar_desconexion_worker(worker_conectado);
            salida=0;
			break;
		default:
			log_warning(storage_logger,"Operacion desconocida. No quieras meter la pata");
			break;
		}
	}

	return NULL;
}

void manejar_desconexion_worker(t_worker* worker){
    pthread_mutex_lock(&mutex_lista_workers);
    list_remove_element(lista_workers, worker);
    log_info_seguro(storage_logger,"##Se desconecta el Worker <%s> - Cantidad de Workers: <%d>", worker->id_worker, list_size(lista_workers));
    pthread_mutex_unlock(&mutex_lista_workers);
}

void enviar_tam_bloque(t_worker* worker){
	t_buffer* buffer = crear_buffer();
	agregar_int_a_buffer(buffer, BLOCK_SIZE);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(ENVIAR_TAM_BLOQUE_STORAGE_WORKER, buffer);
	enviar_paquete(paquete, worker->socket);
	log_debug_seguro(storage_logger, "SE ENVIO TAM BLOQUE=%d a worker=%s", BLOCK_SIZE, worker->id_worker);
	liberar_paquete(paquete);
}

void responder_op_create(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* nombre_file = extraer_string(buffer);
	char* tag = extraer_string(buffer);

	char* resultado=crear_file_tag(nombre_file,tag);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){//HAGA STRCMP CON OPERACION EXITOSA
	log_info_seguro(storage_logger, "##%d - File Creado %s:%s", query_id, nombre_file, tag);

	}
	else {
		log_error_seguro(storage_logger, "##%d - File no pudo ser Creado %s:%s", query_id, nombre_file, tag);

	}
	t_buffer* buffer_respuesta=crear_buffer();
	agregar_string_a_buffer(buffer_respuesta,resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(nombre_file);
	free(tag);
	liberar_buffer(buffer);
}

void responder_op_truncate(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* nombre_file = extraer_string(buffer);
	char* tag = extraer_string(buffer);
	int tamanio = extraer_int(buffer);
	// falta logica
	char* resultado=truncate_file_tag(nombre_file,tag,tamanio,query_id);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){
		log_info_seguro(storage_logger, "##<%d> - File Truncado %s:%s - Tamaño: %d", query_id, nombre_file, tag, tamanio);
	}
	//else{ resultado=0;}
	t_buffer* buffer_respuesta=crear_buffer();
	agregar_string_a_buffer(buffer_respuesta,resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	log_debug_seguro(storage_logger,"cod_op enviado para truncate:%d",RESPUESTA_RESULTADO);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(nombre_file);
	free(tag);
	liberar_buffer(buffer);
}

void responder_op_write(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* nombre_file = extraer_string(buffer);
	char* tag = extraer_string(buffer);
	int nro_bloque_logico = extraer_int(buffer);
	char* contenido = extraer_string(buffer);

	char* resultado=write_block(nombre_file,tag,nro_bloque_logico,contenido,query_id);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){
		//resultado=1;
	}
	//else resultado=0;
	t_buffer* buffer_respuesta= crear_buffer();
	agregar_string_a_buffer(buffer_respuesta,resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(nombre_file);
	free(tag);
	free(contenido);
	liberar_buffer(buffer);
}

void responder_op_tag(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* file_origen = extraer_string(buffer);
	char* tag_origen = extraer_string(buffer);
	char* file_destino = extraer_string(buffer);
	char* tag_destino = extraer_string(buffer);

	char* resultado = copy_tag_file(file_origen, tag_origen, file_destino, tag_destino, query_id);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){
		//resultado = 1;
	}

	t_buffer* buffer_respuesta = crear_buffer();
	agregar_string_a_buffer(buffer_respuesta, resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(file_origen);
	free(tag_origen);
	free(file_destino);
	free(tag_destino);
	liberar_buffer(buffer);
}

void responder_op_commit(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* nombre_file = extraer_string(buffer);
	char* tag = extraer_string(buffer);

	char* resultado=commit_file_tag(nombre_file, tag, query_id);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){
		log_info_seguro(storage_logger, "##<%d> - Commit de File:Tag <%s>:<%s>", query_id, nombre_file, tag);
	}
	//else resultado=0;
	t_buffer* buffer_respuesta= crear_buffer();
	agregar_string_a_buffer(buffer_respuesta,resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(nombre_file);
	free(tag);
	liberar_buffer(buffer);
}

void responder_op_delete(t_worker* worker){
	t_buffer* buffer = recibir_buffer(worker->socket);
	int query_id = extraer_int(buffer);
	char* nombre_file = extraer_string(buffer);
	char* tag = extraer_string(buffer);

	char* resultado = eliminar_tag(nombre_file, tag, query_id);
	if(strcasecmp(resultado,"OPERACION EXITOSA")==0){
		log_info_seguro(storage_logger, "##<%d> - Tag Eliminado <%s>:<%s>", query_id, nombre_file, tag);
	}

	t_buffer* buffer_respuesta = crear_buffer();
	agregar_string_a_buffer(buffer_respuesta, resultado);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_RESULTADO, buffer_respuesta);
	enviar_paquete(paquete, worker->socket);
	liberar_paquete(paquete);
	free(nombre_file);
	free(tag);
	liberar_buffer(buffer);
}


void responder_op_pedir_pagina(t_worker* worker_conectado) {
    t_buffer* buffer = recibir_buffer(worker_conectado->socket);
    int query_id = extraer_int(buffer);
    char* nombre_file = extraer_string(buffer);
    char* tag = extraer_string(buffer);
    int bloque_logico = extraer_int(buffer);

    t_resultado_leer* resultado = read_block(nombre_file, tag, bloque_logico, query_id);

    t_buffer* buffer_respuesta = crear_buffer();
    agregar_int_a_buffer(buffer_respuesta, resultado->resultado);
    agregar_string_a_buffer(buffer_respuesta, resultado->mensaje);

    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_PAGINA, buffer_respuesta);
    enviar_paquete(paquete, worker_conectado->socket);

    if (resultado->resultado != 1) {
        log_error_seguro(storage_logger, "##%d - Error al leer página %s:%s bloque %d", query_id, nombre_file, tag, bloque_logico);
    } else {
        log_info_seguro(storage_logger, "##%d - Pagina respondida %s:%s. contenido:%s", query_id, nombre_file, tag, resultado->mensaje);
    }

    liberar_paquete(paquete);
    liberar_buffer(buffer);

    free(nombre_file);
    free(tag);
    free(resultado->mensaje);
    free(resultado);

    log_debug_seguro(storage_logger, "##%d - Recursos liberados correctamente en PEDIR_PAGINA", query_id);
}
