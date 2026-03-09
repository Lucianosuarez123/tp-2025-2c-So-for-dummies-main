#include <../includes/utils.h>

pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;

// Función base para formatear y loguear
void log_formateado_seguro(void (*log_func)(t_log*, const char*, ...), t_log* logger, const char* formato, va_list args) {
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), formato, args);
    log_func(logger, "%s", buffer);
}

// log_info_seguro
void log_info_seguro(t_log* logger, const char* formato, ...) {
    pthread_mutex_lock(&mutex_log);
    va_list args;
    va_start(args, formato);
    log_formateado_seguro(log_info, logger, formato, args);
    va_end(args);
    pthread_mutex_unlock(&mutex_log);
}

// log_debug_seguro
void log_debug_seguro(t_log* logger, const char* formato, ...) {
    pthread_mutex_lock(&mutex_log);
    va_list args;
    va_start(args, formato);
    log_formateado_seguro(log_debug, logger, formato, args);
    va_end(args);
    pthread_mutex_unlock(&mutex_log);
}

// log_warning_seguro
void log_warning_seguro(t_log* logger, const char* formato, ...) {
    pthread_mutex_lock(&mutex_log);
    va_list args;
    va_start(args, formato);
    log_formateado_seguro(log_warning, logger, formato, args);
    va_end(args);
    pthread_mutex_unlock(&mutex_log);
}

// log_error_seguro
void log_error_seguro(t_log* logger, const char* formato, ...) {
	pthread_mutex_lock(&mutex_log);
    va_list args;
    va_start(args, formato);
    log_formateado_seguro(log_error, logger, formato, args);
    va_end(args);
    pthread_mutex_unlock(&mutex_log);
}


// =======================
// FUNCIONES DE CONEXIÓN
// =======================

int recibir_operacion(int socket_cliente) {
	int cod_op;
	if (recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL) > 0)
		return cod_op;
	else {
		close(socket_cliente);
		return -1;
	}
}

// CLIENTE
int crear_conexion_cliente(char *ip, char* puerto) {
	struct addrinfo hints;
	struct addrinfo *server_info;
	int err;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	err = getaddrinfo(ip, puerto, &hints, &server_info);
	if (err != 0) {
		perror("Error en getaddrinfo");
		return -1;
	}

	int socket_cliente = socket(server_info->ai_family, server_info->ai_socktype, server_info->ai_protocol);
	if (socket_cliente == -1) {
		perror("Error al crear socket");
		freeaddrinfo(server_info);
		return -1;
	}

	err = connect(socket_cliente, server_info->ai_addr, server_info->ai_addrlen);
	if (err != 0) {
		perror("Error en connect");
		close(socket_cliente);
		freeaddrinfo(server_info);
		return -1;
	}

	freeaddrinfo(server_info);
	return socket_cliente;
}

// SERVIDOR
int iniciar_servidor(char* puerto, t_log* logger) {
	int socket_servidor;
	int err;
	struct addrinfo hints, *servinfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	err = getaddrinfo(NULL, puerto, &hints, &servinfo);
	if (err != 0) {
		log_error_seguro(logger, "Error en getaddrinfo: %s", gai_strerror(err));
		return -1;
	}

	socket_servidor = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
	if (socket_servidor == -1) {
		log_error_seguro(logger, "Error al crear socket");
		freeaddrinfo(servinfo);
		return -1;
	}

	err = setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
	if (err == -1) {
		log_error_seguro(logger, "Error en setsockopt");
		close(socket_servidor);
		freeaddrinfo(servinfo);
		return -1;
	}

	err = bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen);
	if (err != 0) {
		log_error_seguro(logger, "Error en bind");
		close(socket_servidor);
		freeaddrinfo(servinfo);
		return -1;
	}

	err = listen(socket_servidor, SOMAXCONN);
	if (err != 0) {
		log_error_seguro(logger, "Error en listen");
		close(socket_servidor);
		freeaddrinfo(servinfo);
		return -1;
	}

	freeaddrinfo(servinfo);
	//log_info_seguro(logger, "Servidor listo para recibir conexiones");

	return socket_servidor;
}

int esperar_cliente(int socket_servidor, t_log* logger) {
	int socket_cliente = accept(socket_servidor, NULL, NULL);
	if (socket_cliente == -1) {
		log_error_seguro(logger, "Error al aceptar cliente");
		return -1;
	}
	return socket_cliente;
}

void liberar_conexion(int socket) {
	close(socket);
}

// =======================
// FUNCIONES DE BUFFER
// =======================

t_buffer* recibir_buffer(int conexion) {
	t_buffer* un_buffer = malloc(sizeof(t_buffer));

	if (recv(conexion, &(un_buffer->size), sizeof(int), MSG_WAITALL) <= 0) {
		free(un_buffer);
		return NULL;
	}

	un_buffer->stream = malloc(un_buffer->size);
	if (recv(conexion, un_buffer->stream, un_buffer->size, MSG_WAITALL) <= 0) {
		free(un_buffer->stream);
		free(un_buffer);
		return NULL;
	}

	return un_buffer;
}

void* extraer_stream(t_buffer* un_buffer) {
	if (un_buffer->size < sizeof(int)) return NULL;

	int size_stream;
	memcpy(&size_stream, un_buffer->stream, sizeof(int));

	if (un_buffer->size < sizeof(int) + size_stream) return NULL;

	void* stream = malloc(size_stream);
	memcpy(stream, un_buffer->stream + sizeof(int), size_stream);

	int nuevo_size = un_buffer->size - sizeof(int) - size_stream;

	void* nuevo_stream = NULL;
	if (nuevo_size > 0) {
	nuevo_stream = malloc(nuevo_size);
	memcpy(nuevo_stream, un_buffer->stream + sizeof(int) + size_stream, nuevo_size);
	}

	free(un_buffer->stream);
	un_buffer->stream = nuevo_stream;
	un_buffer->size = nuevo_size;

	return stream;
}


int extraer_int(t_buffer* un_buffer) {
	int* valor_entero = extraer_stream(un_buffer);
	int valor = *valor_entero;
	free(valor_entero);
	return valor;
}

char* extraer_string(t_buffer* un_buffer) {
	char* string = extraer_stream(un_buffer);
	char* copia = strdup(string);
	free(string);
	return copia;
}

uint32_t extraer_uint32(t_buffer* un_buffer) {
	uint32_t* valor = extraer_stream(un_buffer);
	uint32_t resultado = *valor;
	free(valor);
	return resultado;
}

void liberar_buffer(t_buffer* un_buffer) {
	if (un_buffer->stream != NULL)
	free(un_buffer->stream);
	free(un_buffer);
}

void liberar_paquete(t_paquete* un_paquete) {
	if (un_paquete->buffer != NULL)
	liberar_buffer(un_paquete->buffer);
	free(un_paquete);
}

t_buffer* crear_buffer() {
	t_buffer* un_buffer = malloc(sizeof(t_buffer));
	un_buffer->size = 0;
	un_buffer->stream = NULL;
	return un_buffer;
}

void agregar_a_buffer(t_buffer* un_buffer, void* valor, int size) {
	un_buffer->stream = realloc(un_buffer->stream, un_buffer->size + sizeof(int) + size);
	memcpy(un_buffer->stream + un_buffer->size, &size, sizeof(int));
	memcpy(un_buffer->stream + un_buffer->size + sizeof(int), valor, size);
	un_buffer->size += sizeof(int) + size;
}

void agregar_int_a_buffer(t_buffer* un_buffer, int valor) {
	agregar_a_buffer(un_buffer, &valor, sizeof(int));
}

void agregar_uint32_a_buffer(t_buffer* un_buffer, uint32_t valor) {
	agregar_a_buffer(un_buffer, &valor, sizeof(uint32_t));
}

void agregar_string_a_buffer(t_buffer* un_buffer, char* string) {
	agregar_a_buffer(un_buffer, string, strlen(string) + 1);
}

t_paquete* crear_paquete_con_codigo_de_operacion(op_code cod_op, t_buffer* un_buffer) {
	t_paquete* un_paquete = malloc(sizeof(t_paquete));
	un_paquete->codigo_operacion = cod_op;
	un_paquete->buffer = un_buffer;
	return un_paquete;
}

void* serializar_paquete(t_paquete* un_paquete) {
	int size = 2 * sizeof(uint32_t) + un_paquete->buffer->size;
	void* stream = malloc(size);
	int offset = 0;

	memcpy(stream + offset, &(un_paquete->codigo_operacion), sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(stream + offset, &(un_paquete->buffer->size), sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(stream + offset, un_paquete->buffer->stream, un_paquete->buffer->size);

	return stream;
	}

	void enviar_paquete(t_paquete* un_paquete, int socket) {
	void* stream = serializar_paquete(un_paquete);
	int size = 2 * sizeof(uint32_t) + un_paquete->buffer->size;
	send(socket, stream, size, 0);
	free(stream);
}
