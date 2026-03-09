#ifndef QUERY_MASTER_H_
#define QUERY_MASTER_H_

#include "globalQuery.h"
void* atender_query_master(void* arg);
void recibir_lectura_de_query(int socket);
void recibir_finalizacion_de_query(int socket);
void enviar_query_a_master(int socket,char* archivo_query, int prioridad);
#endif