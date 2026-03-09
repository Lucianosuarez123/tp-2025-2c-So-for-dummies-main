#ifndef GLOBALMASTER_H_
#define GLOBALMASTER_H_

#include <../../utils/includes/utils.h>
#include <semaphore.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>
typedef enum {
    READY,
    EXEC,
    EXIT
} EstadoQuery;
typedef enum {
    ORDEN_MUTEX_ID_QUERY = 0,
    ORDEN_MUTEX_LISTA_QUERYS,
    ORDEN_MUTEX_READY,
    ORDEN_MUTEX_EXEC,
    ORDEN_MUTEX_EXIT,
    ORDEN_MUTEX_LISTA_WORKERS,
    ORDEN_MUTEX_COUNT
} OrdenMutexMaster;

typedef struct {
    int id_query;
    int socket;
    char* archivo_query;
    int prioridad;
    int program_counter;
    EstadoQuery estado_actual;
    bool esperando_desalojo;
    pthread_mutex_t mutex_query;
    bool finalizo;



    uint64_t ultimo_aging; // timestamp en ms cuando entró a READY o se actualizó


} t_query;
typedef struct {
    int socket;
    char* id_worker;
    bool ocupado;
    t_query* query_asignada;
} t_worker;

extern t_list* cola_ready;
extern t_list* cola_exec;

extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_exec;
extern pthread_mutex_t mutex_exit;

extern sem_t sem_queries_en_ready;
extern sem_t sem_worker_libre;
extern sem_t sem_eventos;

extern char* PUERTO_ESCUCHA;
extern char* ALGORITMO_PLANIFICACION;
extern int TIEMPO_AGING;
extern char* LOG_LEVEL;


extern t_log* master_logger;
extern t_config* master_config;

extern t_list* lista_workers;
extern pthread_mutex_t mutex_lista_workers;
extern t_list* lista_querys;
extern pthread_mutex_t mutex_lista_querys;
extern pthread_mutex_t mutex_id_query;
extern int id_query;

t_query* crear_query(int nuevo_cliente_fd, char* archivo_query, int prioridad);
bool mover_query_seguro(t_query* query, EstadoQuery origen, EstadoQuery destino);
t_list* obtener_cola_por_estado(EstadoQuery estado);
const char* estado_a_string(EstadoQuery estado);
t_worker* obtener_worker_libre();
bool comparar_query_por_prioridad_con_aging(void* a, void* b);
void ordenar_cola_ready();
t_worker* buscar_worker_por_query_id(int id_query);
t_worker* buscar_worker_por_id(char* id_worker);
t_query* buscar_query_en_cola_por_id(int id_query, t_list* cola);
void destruir_query(t_query* query);
void destruir_worker(t_worker* worker);
uint64_t get_time_ms();
t_query* proxima_query_a_envejecer();
void aplicar_aging(t_query* q);
#endif