#ifndef QUERY_INTERPRETER_H_
#define QUERY_INTERPRETER_H_

#include "globalWorker.h"
#include "gestion_memoria_worker.h"
#include "paginacion_worker.h"
#include "worker_master.h"
t_list* obtener_instrucciones_proceso(char* nombre_archivo_query);
char* leer_palabra(char* instruccion, int palabra_numero);
void interpretar_instrucciones(t_list* instrucciones, int pc_inicial);
bool ejecutar_create(char* instruccion);
bool ejecutar_truncate(char* instruccion);
bool ejecutar_write(char* instruccion);
bool ejecutar_read(char* instruccion);
bool ejecutar_tag(char* instruccion);
bool ejecutar_commit(char* instruccion);
bool ejecutar_flush(char* instruccion);
bool ejecutar_delete(char* instruccion);
bool ejecutar_end(char* instruccion);
void* ejecutar_query(void* args);

bool checkear_interrupcion(int pc_actual);

#endif