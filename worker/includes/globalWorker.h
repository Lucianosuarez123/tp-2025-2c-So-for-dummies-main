#ifndef GLOBALWORKER_H_
#define GLOBALWORKER_H_

#include "../../utils/includes/utils.h"
#include "../../utils/includes/codigo_operacion.h"
#include <commons/bitarray.h>
#include <sys/time.h>
#include <stdint.h>
// Estructura de la Query
typedef struct {
    int id_query;
    int pc_inicial;
    char* archivo_query;
} t_query;

extern void* memoria_interna;
extern int cantidad_marcos;
extern t_bitarray* bitmap_marcos;
extern int puntero_clock;
extern t_dictionary* tablas_de_paginas;
typedef struct {
    int numero_pagina;
    int marco;
    char* file_tag;
    bool modificada;
    bool presente;
    uint64_t timestamp; // para LRU
    bool uso;           // para CLOCK-M
    bool en_reemplazo;  // indicador para evitar reemplazos concurrentes sobre la misma página
} t_pagina;


typedef struct{
    int query_id;
    bool hay_interrupcion;
}t_interrupcion;
typedef struct{
    int resultado;
    char* contenido;
}t_respuesta_global_de_lectura_bloque;

extern t_interrupcion interrupcion;
extern t_respuesta_global_de_lectura_bloque respuesta_global_de_lectura_bloque;
// Variables globales de configuración
extern char* IP_MASTER;
extern char* PUERTO_MASTER;
extern char* IP_STORAGE;
extern char* PUERTO_STORAGE;
extern char* LOG_LEVEL;
extern int TAM_MEMORIA;
extern int RETARDO_MEMORIA;
extern char* ALGORITMO_REEMPLAZO;
extern char* PATH_SCRIPTS;

// Recursos compartidos
extern int fd_master;
extern int fd_storage;
extern int block_size;

extern t_log* worker_logger;
extern t_config* worker_config;

extern char* archivo_config;
extern char* ID_Worker;

extern pthread_t hilo_ejecutar_query;
extern bool hilo_ejecutar_activo;
extern pthread_mutex_t mutex_hilo_ejecutar;

extern pthread_mutex_t mutex_interrupcion;
extern pthread_mutex_t mutex_resultado_global_lectura_bloque;
extern bool end_enviado;
// Mutexes para proteger memoria, bitmap y tablas de páginas
extern pthread_mutex_t mutex_memoria;
extern pthread_mutex_t mutex_bitmap;
extern pthread_mutex_t mutex_tablas;
extern pthread_mutex_t mutex_fd_storage;

extern int query_id_actual;
void buscar_en_tabla(char* key, void* value);
void safe_dictionary_iterator(t_dictionary* diccionario, void (*callback)(char* key, void* value));
#endif