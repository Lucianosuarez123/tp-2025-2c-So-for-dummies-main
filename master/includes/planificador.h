#ifndef PLANIFICADOR_H_
#define PLANIFICADOR_H_

#include "globalMaster.h"
#include "master_worker.h"
void* planificador(void* _);
void* planificador_fifo(void* _);
void* planificador_con_desalojo(void* _);
void* query_menor_prioridad_que_otra(void* a, void* b);

#endif