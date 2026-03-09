#ifndef MASTER_WORKER_H_
#define MASTER_WORKER_H_

#include "globalMaster.h"
#include "master_query.h"
void* atender_master_worker(void* arg);
void enviar_query_a_worker(t_worker* worker, t_query* query);
void enviar_interrupcion_a_worker(int socket, int id_query);
void recibir_respuesta_interrupcion(int socket, char* id_worker);
void recibir_finalizacion_de_query(int socket_worker,char* id_worker);
void  recibir_lectura_realizada(int socket_worker,char* id_worker);
void manejar_desconexion_worker(char* id_worker, int socket);
#endif