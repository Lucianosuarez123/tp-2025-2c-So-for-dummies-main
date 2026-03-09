#ifndef WORKER_MASTER_H_
#define WORKER_MASTER_H_
#include <../includes/globalWorker.h>
#include <../includes/query_interpreter.h>
void* atender_worker_master(void* arg);
void iniciar_ejecucion_query(int socket);
void recibir_interrupcion(int socket);
void enviar_respuesta_interrupcion(int fd_master, int pc, int query_id);
#endif