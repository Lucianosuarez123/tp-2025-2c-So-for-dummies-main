
#ifndef TAG_LOCK_MANAGER_H
#define TAG_LOCK_MANAGER_H

#include "globalStorage.h"
#include <pthread.h>
#include <commons/collections/dictionary.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

// Destruye todos los locks (al cerrar el Storage)
void tag_lock_manager_destroy(void);

// Bloquea el mutex del File:Tag (crea si no existe)
void lock_tag(const char* file, const char* tag);

// Desbloquea el mutex del File:Tag
void unlock_tag(const char* file, const char* tag);

// Elimina el mutex del File:Tag (cuando se borra el tag)
void remove_tag_lock(const char* file, const char* tag);

#endif // TAG_LOCK_MANAGER_H
