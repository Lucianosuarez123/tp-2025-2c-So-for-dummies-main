#include <../includes/query_master.h>

void* atender_query_master(void* arg){
    int socket=fd_master;
	bool salida = 1;
	while (salida) { // Ahora espera reconexiones
        int cod_op = recibir_operacion(socket);
		switch (cod_op) {
		case LECTURA_REALIZADA:
            recibir_lectura_de_query(socket);
		break;
        case FINALIZACION_DE_QUERY:
            recibir_finalizacion_de_query(socket);
            salida=0;
            break;
		case DESCONEXION:
			log_debug_seguro(query_logger,"se desconecto master");
            salida=0;
			break;
		default:
			//log_warning(query_logger,"Operacion desconocida. No quieras meter la pata");
			break;
		}
	}

	return NULL;
}


void enviar_query_a_master(int socket,char* archivo_query, int prioridad){
    t_buffer* buffer=crear_buffer();
    agregar_int_a_buffer(buffer, prioridad);
    agregar_string_a_buffer(buffer, archivo_query);
    t_paquete* paquete = crear_paquete_con_codigo_de_operacion(ENVIAR_QUERY_A_MASTER, buffer);
    enviar_paquete(paquete, socket);
    log_info_seguro(query_logger,"## Solicitud de ejecución de Query: %s, prioridad: %d", archivo_query, prioridad);
    liberar_paquete(paquete);
}

void recibir_lectura_de_query(int socket){
    t_buffer* buffer= recibir_buffer(socket);
    char* file_y_tag = extraer_string(buffer);
    char* contenido = extraer_string(buffer);
    log_info_seguro(query_logger,"## Lectura realizada: Archivo %s, contenido: %s", file_y_tag, contenido);
    free(file_y_tag);
    free(contenido);
    liberar_buffer(buffer);
}

void recibir_finalizacion_de_query(int socket){
    t_buffer* buffer= recibir_buffer(socket);
    char* motivo = extraer_string(buffer);
    log_info_seguro(query_logger,"## Query Finalizada - %s", motivo);
    free(motivo);
    liberar_buffer(buffer);
}
