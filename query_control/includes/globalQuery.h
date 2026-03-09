#ifndef GLOBALQUERY_H_
#define GLOBALQUERY_H_

#include <../../utils/includes/utils.h>


extern char* IP_MASTER;
extern char* PUERTO_MASTER;
extern char* LOG_LEVEL;

extern int fd_master;
extern t_log* query_logger;
extern t_config* query_config;

extern char* archivo_query;
extern char* archivo_config;
extern int prioridad;
#endif