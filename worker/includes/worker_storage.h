#ifndef WORKER_STORAGE_H_
#define WORKER_STORAGE_H_

#include "globalWorker.h"

bool flush_pagina(char* file_tag, t_pagina* pagina, int id_query);
char* pedir_contenido_storage(char* file_tag, int numero_pagina);

#endif