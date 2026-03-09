
#include "../includes/tag_locks.h"


/*
 * Diccionario global:
 *  clave: "<file>/<tag>"
 *  valor: pthread_mutex_t*
 */

/* Construye una clave normalizada "<file>/<tag>" (caller debe free()) */
static char* make_key(const char* file, const char* tag) {
    return string_from_format("%s/%s", file, tag);
}


/* Destruye todos los locks y el diccionario (al cerrar el Storage) */
void tag_lock_manager_destroy(void) {
    if (!tag_locks) return;
    void _destroy_lock(void* lock) {
        pthread_mutex_destroy((pthread_mutex_t*)lock);
        free(lock);
    }
    dictionary_destroy_and_destroy_elements(tag_locks, _destroy_lock);
    tag_locks = NULL;
}

/* Bloquea el mutex del File:Tag (si no existe, lo crea y lo inserta) */

void lock_tag(const char* file, const char* tag) {
    if (!tag_locks) return;
    char* key = make_key(file, tag);

    pthread_mutex_lock(&g_tag_map_mutex);
    pthread_mutex_t* lock = dictionary_get(tag_locks, key);
    if (!lock) {
        lock = malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(lock, NULL);

        // El diccionario duplica/gestiona su propia copia del string
        dictionary_put(tag_locks, key, lock);
    }
    pthread_mutex_unlock(&g_tag_map_mutex);

    // Mantener la liberación si el lock ya existía (key temporal de búsqueda)
    free(key);

    pthread_mutex_lock(lock);
}


/* Desbloquea el mutex del File:Tag */
void unlock_tag(const char* file, const char* tag) {
    if (!tag_locks) return;  // defensivo
    char* key = make_key(file, tag);

    pthread_mutex_lock(&g_tag_map_mutex);
    pthread_mutex_t* lock = dictionary_get(tag_locks, key);
    pthread_mutex_unlock(&g_tag_map_mutex);

    free(key);  // key temporal para la búsqueda

    if (lock) {
        pthread_mutex_unlock(lock);
    }
    // Si no existe, es un uso incorrecto (unlock sin lock previo); lo ignoramos de forma segura.
}

/* Elimina el mutex del File:Tag (cuando se borra el tag) */
void remove_tag_lock(const char* file, const char* tag) {
    if (!tag_locks) return;  // defensivo
    char* key = make_key(file, tag);

    pthread_mutex_lock(&g_tag_map_mutex);
    pthread_mutex_t* lock = dictionary_remove(tag_locks, key);
    pthread_mutex_unlock(&g_tag_map_mutex);

    free(key);  // key temporal de la operación remove

    if (lock) {
        // Destruir el mutex y liberar su memoria
        pthread_mutex_destroy(lock);
        free(lock);
    }
    // Si no había lock, simplemente no hacemos nada (pudo no haberse creado aún).
}
