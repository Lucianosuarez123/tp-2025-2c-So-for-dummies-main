#ifndef ALGORITMOS_REEMPLAZO_H_
#define ALGORITMOS_REEMPLAZO_H_

#include "globalWorker.h"
#include "worker_storage.h"
t_pagina* seleccionar_por_clock_modificado();
t_pagina* seleccionar_por_lru();
t_pagina* seleccionar_pagina_para_reemplazo();
int reemplazar_pagina(int query_id, char* file_tag,int nro_pagina_a_colocar_en_memoria);
void buscar_modificadas(char* key, void* value);
#endif