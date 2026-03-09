#ifndef MASTER_QUERY_H_
#define MASTER_QUERY_H_

#include "globalMaster.h"
#include "master_worker.h"
void* atender_master_query(void* arg);
void manejar_desconexion_query(int id_query, int socket);
void enviar_finalizacion_query_a_query_control(int socket, char* motivo);
void enviar_lectura_a_query_control(int socket,int query_id, char* id_worker, char* file_y_tag, char* contenido);
#endif