#ifndef PAGINACION_WORKER_H_
#define PAGINACION_WORKER_H_

#include "globalWorker.h"
#include "worker_storage.h"
#include "gestion_memoria_worker.h"
#include "algoritmos_reemplazo.h"

void cargar_pagina(int query_id, char* file_tag, int numero_pagina);
t_pagina* buscar_pagina(char* file_tag, int numero_pagina);
// Safe: toma y libera `mutex_tablas` internamente.
t_pagina* buscar_pagina_por_marco(int marco);
// Unsafe: asume que el llamador posee `mutex_tablas`.
t_pagina* buscar_pagina_por_marco_unsafe(int marco);
// Helpers para actualizar bitmap y metadatos de página de forma atómica
// `asignar_marco_a_pagina` intenta setear el bit del marco y actualiza
// los metadatos; devuelve 0 en éxito, -1 si el marco ya estaba ocupado.
int asignar_marco_a_pagina(t_pagina* pagina, int marco, int query_id, char* file_tag);
void liberar_marco_de_pagina(t_pagina* pagina, int marco, int query_id, char* file_tag);
void verificar_consistencia_bitmap_tablas(void);

#endif