#ifndef GESTION_MEMORIA_WORKER_H_
#define GESTION_MEMORIA_WORKER_H_

#include "globalWorker.h"
#include "paginacion_worker.h"



void inicializar_memoria_interna();
int buscar_marco_libre(t_bitarray* bitmap);
bool bitarray_hay_marco_libre(t_bitarray* bitmap);
uint64_t get_timestamp();
char* leer_memoria(int query_id, int marco, int offset, int tamanio);
void  escribir_memoria(int query_id, int marco, int offset, char* contenido);
void dump_memoria();



#endif