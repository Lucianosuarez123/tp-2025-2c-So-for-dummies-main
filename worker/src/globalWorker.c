#include "../includes/globalWorker.h"

void* memoria_interna = NULL;
int cantidad_marcos = 0;
t_bitarray* bitmap_marcos = NULL;
int puntero_clock = 0;
t_dictionary* tablas_de_paginas= NULL;



void safe_dictionary_iterator(t_dictionary* diccionario, void (*callback)(char* key, void* value)) {
	if (diccionario == NULL || callback == NULL) return;

	pthread_mutex_lock(&mutex_tablas);
	t_list* claves = dictionary_keys(diccionario);
	for (int i = 0; i < list_size(claves); i++) {
		char* key = list_get(claves, i);
		void* value = dictionary_get(diccionario, key);
		callback(key, value);
	}
	list_destroy(claves);
	pthread_mutex_unlock(&mutex_tablas);
}
