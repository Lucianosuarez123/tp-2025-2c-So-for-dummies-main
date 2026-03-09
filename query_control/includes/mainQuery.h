#ifndef MAINQUERY_H_
#define MAINQUERY_H_

#include "globalQuery.h"
#include "iniciarQuery.h"
#include "query_master.h"

char* IP_MASTER;
char* PUERTO_MASTER;
char* LOG_LEVEL;

char* archivo_query;
char* archivo_config;
int prioridad;
int fd_master;
t_log* query_logger;
t_config* query_config;
#endif