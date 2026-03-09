#ifndef STORAGEGLOBAL_H_
#define STORAGEGLOBAL_H_

#include <../../utils/includes/utils.h>

#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <commons/bitarray.h>
#include <commons/string.h>
#include <sys/mman.h>
#include <errno.h>
#include <dirent.h>
typedef struct
{
    int socket;
    char* id_worker;

} t_worker;


extern char* PUERTO_ESCUCHA;
extern char* FRESH_START;
extern char* PUNTO_MONTAJE;
extern int RETARDO_OPERACION;
extern int RETARDO_ACCESO_BLOQUE;
extern char* LOG_LEVEL;
extern int FS_SIZE;
extern int BLOCK_SIZE;


extern t_log* storage_logger;
extern t_config* storage_config;
extern t_config* superblock_config;

extern char* archivo_config;

extern pthread_mutex_t mutex_lista_workers;
extern t_list* lista_workers;
extern int puerto_escucha_fd;

extern t_dictionary* tag_locks;
extern pthread_mutex_t mutex_fs;           // Para operaciones sobre el sistema de archivos lógico
extern pthread_mutex_t mutex_bitmap;       // Para acceso al bitmap
extern pthread_mutex_t mutex_blocks;       // Para acceso a bloques físicos
extern pthread_mutex_t mutex_hash_index;   // Para acceso al archivo de hashes
extern pthread_mutex_t g_tag_map_mutex;
// Bitmap
extern t_bitarray* bitmap;
extern void* bitmap_mem;
extern int fd_bitmap;

// Paths
extern char path_bitmap[PATH_MAX];
extern char path_blocks[PATH_MAX];
extern char path_files[PATH_MAX];
extern char path_hash_index[PATH_MAX];
extern char path_superblock[PATH_MAX];



void free_string_array(char** arr);
#endif