#ifndef MAINSTORAGE_H_
#define MAINSTORAGE_H_

#include "globalStorage.h"
#include "iniciarStorage.h"
#include "storage_worker.h"
char* PUERTO_ESCUCHA;
char* FRESH_START;
char* PUNTO_MONTAJE;
int RETARDO_OPERACION=-1;
int RETARDO_ACCESO_BLOQUE=-1;
char* LOG_LEVEL;
int FS_SIZE=-1;
int BLOCK_SIZE=-1;

char* archivo_config;


t_log* storage_logger;
t_config* storage_config;
t_config* superblock_config;
pthread_mutex_t mutex_lista_workers;
t_list* lista_workers;

int puerto_escucha_fd;

t_dictionary* tag_locks;
pthread_mutex_t mutex_fs;           // Para operaciones sobre el sistema de archivos lógico
pthread_mutex_t mutex_bitmap;       // Para acceso al bitmap
pthread_mutex_t mutex_blocks;       // Para acceso a bloques físicos
pthread_mutex_t mutex_hash_index;   // Para acceso al archivo de hashes
pthread_mutex_t g_tag_map_mutex;
// Bitmap
t_bitarray* bitmap;
void* bitmap_mem;
int fd_bitmap;
// Paths
char path_bitmap[PATH_MAX];
char path_blocks[PATH_MAX];
char path_files[PATH_MAX];
char path_hash_index[PATH_MAX];
char path_superblock[PATH_MAX];


void* gestionar_conexiones(void* _);
t_worker* crear_worker(int socket, char* id_worker);
#endif