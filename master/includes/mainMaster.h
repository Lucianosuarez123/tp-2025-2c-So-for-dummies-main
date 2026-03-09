#ifndef MAINMASTER_H_
#define MAINMASTER_H_

#include "globalMaster.h"
#include "iniciarMaster.h"
#include "master_query.h"
#include "master_worker.h"
#include "planificador.h"
char* PUERTO_ESCUCHA;
char* ALGORITMO_PLANIFICACION;
int TIEMPO_AGING=-1;
char* LOG_LEVEL;

int puerto_escucha_fd;
char* archivo_config;

t_log* master_logger;
t_config* master_config;

t_list* lista_workers;
t_list* lista_querys;
pthread_mutex_t mutex_lista_workers;
pthread_mutex_t mutex_lista_querys;
pthread_mutex_t mutex_id_query;
int id_query=0;


t_list* cola_ready;
t_list* cola_exec;

pthread_mutex_t mutex_ready;
pthread_mutex_t mutex_exec;
pthread_mutex_t mutex_exit;

sem_t sem_queries_en_ready;
sem_t sem_worker_libre;
sem_t sem_eventos;
void* gestionar_conexiones(void* _);
#endif