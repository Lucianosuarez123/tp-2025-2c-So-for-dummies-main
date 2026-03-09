#ifndef STORAGE_WORKER_H_
#define STORAGE_WORKER_H_

#include "globalStorage.h"
#include "gestion_storage.h"
void* atender_storage_worker(void* arg);
void manejar_desconexion_worker(t_worker* worker);
void enviar_tam_bloque(t_worker* worker);
void responder_op_create(t_worker* worker);
void responder_op_truncate(t_worker* worker);
void responder_op_write(t_worker* worker);
void responder_op_read(t_worker* worker);
void responder_op_tag(t_worker* worker);
void responder_op_commit(t_worker* worker);
void responder_op_delete(t_worker* worker);
void responder_op_pedir_pagina(t_worker* worker_conectado);
#endif