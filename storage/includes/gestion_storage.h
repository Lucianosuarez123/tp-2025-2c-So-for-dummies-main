#ifndef GESTION_STORAGE_H_
#define GESTION_STORAGE_H_

#include "globalStorage.h"
#include "commons/crypto.h"
#include "tag_locks.h"
typedef struct{
    int resultado;
    char* mensaje;
}t_resultado_leer;
int contar_referencias_bloque(int bloque_fisico);
void liberar_bloque_fisico(int bloque);
int reservar_bloque_fisico_libre();
void aplicar_retardo_op();
void aplicar_retardo_block();
char* crear_file_tag(const char* nombre_file, const char* nombre_tag);
char* truncate_file_tag(const char* nombre_file, const char* nombre_tag, int nuevo_tamanio, int query_id);
char* write_block(const char* nombre_file, const char* nombre_tag, int bloque_logico, const char* contenido, int query_id);
t_resultado_leer* read_block(const char* nombre_file, const char* nombre_tag, int bloque_logico, int query_id);
char* commit_file_tag(const char* nombre_file, const char* nombre_tag, int query_id);
char* copy_tag_file(const char* file_origen, const char* tag_origen, const char* file_destino, const char* tag_destino, int query_id);
char* eliminar_tag(const char* nombre_file, const char* nombre_tag, int query_id);
#endif