#include <../includes/worker_master.h>


void* atender_worker_master(void* arg){
    int socket = fd_master;
	bool salida = 1;
    log_debug_seguro(worker_logger,"comenzando a atender master");
	while (salida) { // Ahora espera reconexiones
        int cod_op = recibir_operacion(socket);
        log_debug_seguro(worker_logger,"cod op=%d", cod_op);
		switch (cod_op) {
        case EJECUTAR_QUERY:
            log_debug_seguro(worker_logger,"SE QUIERE EJECUTAR UNA QUERY");
            iniciar_ejecucion_query(socket);
		// case <insertar cod_op>:
        //   funcion_que_utiliza_buffer_del_mensaje(worker_conectado);
		break;
        case INTERRUPCION:
            recibir_interrupcion(socket);
            break;
		case DESCONEXION:
			log_debug_seguro(worker_logger,"se desconecto storage");
            //manejar_desconexion_worker(worker_conectado);
            salida=0;
			break;
		default:
			log_warning(worker_logger,"Operacion desconocida. No quieras meter la pata");
			break;
		}
	}

    log_debug_seguro(worker_logger, "mensaje");

	return NULL;
}

void iniciar_ejecucion_query(int socket){

    t_buffer* buffer = recibir_buffer(socket);
    if (!buffer) {
        log_error_seguro(worker_logger, "Error al recibir la query.");
        return;
    }

    int id_query = extraer_int(buffer);
    int pc_inicial = extraer_int(buffer);
    char* archivo_query = extraer_string(buffer);
    char* ruta_completa = string_from_format("%s/%s", PATH_SCRIPTS, archivo_query);
    log_info_seguro(worker_logger, "## Query <%d>: Se recibe la Query. El path de operaciones es: <%s>",id_query, ruta_completa);
    free(ruta_completa);
    pthread_mutex_lock(&mutex_hilo_ejecutar);
    if (hilo_ejecutar_activo) {
        pthread_mutex_unlock(&mutex_hilo_ejecutar); // liberamos para evitar deadlock en join
        pthread_join(hilo_ejecutar_query, NULL);  // esperamos a que termine el hilo anterior
        pthread_mutex_lock(&mutex_hilo_ejecutar);   // volvemos a tomar el mutex
        hilo_ejecutar_activo = false;
    }



    t_query* args = malloc(sizeof(t_query));
    args->archivo_query = archivo_query;
    args->id_query=id_query;
    args->pc_inicial=pc_inicial;

    // reset flag that indicates if an OP_END was already sent for this query
    end_enviado = false;


    if (pthread_create(&hilo_ejecutar_query, NULL, (void*) ejecutar_query, args) != 0) {
        log_error_seguro(worker_logger, "Error al crear el hilo de ejecución.");
        free(archivo_query);
        free(args);
        pthread_mutex_unlock(&mutex_hilo_ejecutar);
        liberar_buffer(buffer);
        return;
    }

    hilo_ejecutar_activo = true;
    pthread_mutex_unlock(&mutex_hilo_ejecutar);
    log_debug_seguro(worker_logger,"SE INICIO HILO PARA EJECUTAR LA QUERY, archivo:%s", args->archivo_query);
    liberar_buffer(buffer);
}

void recibir_interrupcion(int socket){
    t_buffer* buffer = recibir_buffer(socket);
    int query_id = extraer_int(buffer);
    pthread_mutex_lock(&mutex_interrupcion);
    interrupcion.hay_interrupcion=true;
    interrupcion.query_id=query_id;
    pthread_mutex_unlock(&mutex_interrupcion);
    log_debug_seguro(worker_logger,"se recibio interrupcion");
    liberar_buffer(buffer);
}

void enviar_respuesta_interrupcion(int fd_master, int pc, int query_id){
    t_buffer* buffer = crear_buffer();
    agregar_int_a_buffer(buffer,query_id);
    agregar_int_a_buffer(buffer,pc);
    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(RESPUESTA_INTERRUPCION, buffer);
    enviar_paquete(paquete, fd_master);
    liberar_paquete(paquete);
    log_debug_seguro(worker_logger,"se envio respuesta de interrupcion");
}