#ifndef MAINWORKER_H_
#define MAINWORKER_H_

#include "globalWorker.h"
#include "iniciarWorker.h"
#include "worker_master.h"
#include "worker_storage.h"
#include "query_interpreter.h"
#include "paginacion_worker.h"
#include "gestion_memoria_worker.h"
#include "algoritmos_reemplazo.h"
char* IP_MASTER;
char* PUERTO_MASTER;
char* IP_STORAGE;
char* PUERTO_STORAGE;
char* LOG_LEVEL;
int TAM_MEMORIA;
int RETARDO_MEMORIA;
char* ALGORITMO_REEMPLAZO;
char* PATH_SCRIPTS;

char* archivo_config;
char* ID_Worker;
int fd_master;
int fd_storage;
t_log* worker_logger;
t_config* worker_config;




t_interrupcion interrupcion;
t_respuesta_global_de_lectura_bloque respuesta_global_de_lectura_bloque;
int query_id_actual;

int block_size;
pthread_t hilo_ejecutar_query;
bool hilo_ejecutar_activo = false;
bool end_enviado = false;
pthread_mutex_t mutex_resultado_global_lectura_bloque;
pthread_mutex_t mutex_hilo_ejecutar;
pthread_mutex_t mutex_interrupcion;
void enviar_id(int socket, char* id_worker);

void handshake_con_storage(int fd_storage, char* ID_Worker);

#endif