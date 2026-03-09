#include <../includes/query_interpreter.h>

// Helper para liberar una estructura t_pagina almacenada en las tablas
static void liberar_pagina(void* elemento) {
	if (!elemento) return;
	t_pagina* p = (t_pagina*)elemento;
	if (p->file_tag) free(p->file_tag);
	free(p);
}


t_list* obtener_instrucciones_proceso(char* nombre_archivo_query) {
	t_list* instrucciones = list_create();

	char* ruta_completa = string_from_format("%s/%s",PATH_SCRIPTS, nombre_archivo_query);
	FILE* archivo = fopen(ruta_completa, "r");

	if (archivo == NULL) {
		log_error_seguro(worker_logger, "No se pudo abrir el archivo de instrucciones: %s", ruta_completa);
		free(ruta_completa);
		return instrucciones;
	}

	char* linea = NULL;
	size_t len = 0;
	ssize_t read;

	while ((read = getline(&linea, &len, archivo)) != -1) {
		char* copia = strdup(linea);              // Copiamos la línea original
		string_trim_right(&copia);                // Trim seguro sobre la copia
		list_add(instrucciones, copia);           // Guardamos la copia limpia
	}

	free(linea);  // buffer de getline
	free(ruta_completa);
	fclose(archivo);
	log_debug_seguro(worker_logger,"CANTIDAD DE INSTRUCCIONES:%d",list_size(instrucciones));
	return instrucciones;
}

char* leer_palabra(char* instruccion, int palabra_numero) {
	// Divide la instrucción por espacios
	char** partes = string_split(instruccion, " ");

	if (partes == NULL || partes[palabra_numero] == NULL) {
		return NULL;
	}

	char* palabra = strdup(partes[palabra_numero]); // Copiamos la primera palabra

	string_array_destroy(partes); // Liberamos el array de strings
	return palabra;
}


void interpretar_instrucciones(t_list* instrucciones, int pc_inicial) {

	for (int i = pc_inicial; i < list_size(instrucciones); i++) {
		char* instruccion = list_get(instrucciones, i);
		char* operacion = leer_palabra(instruccion, 0);
		log_info_seguro(worker_logger, "## Query <%d>: FETCH - Program Counter: <%d> - <%s>", query_id_actual, i, operacion);
		if (strcmp(operacion, "CREATE") == 0) {
			if(!ejecutar_create(instruccion)) break;
		} else if (strcmp(operacion, "TRUNCATE") == 0) {
			if(!ejecutar_truncate(instruccion)) break;
		} else if (strcmp(operacion, "WRITE") == 0) {
			if(!ejecutar_write(instruccion)) break;
		} else if (strcmp(operacion, "READ") == 0) {
			if(!ejecutar_read(instruccion)) break;
		} else if (strcmp(operacion, "TAG") == 0) {
			if(!ejecutar_tag(instruccion)) break;
		} else if (strcmp(operacion, "COMMIT") == 0) {
			if(!ejecutar_commit(instruccion)) break;
		} else if (strcmp(operacion, "FLUSH") == 0) {
			if(!ejecutar_flush(instruccion)) break;
		} else if (strcmp(operacion, "DELETE") == 0) {
			if(!ejecutar_delete(instruccion)) break;
		} else if (strcmp(operacion, "END") == 0) {
			ejecutar_end(instruccion);
			log_info_seguro(worker_logger,"## Query <%d>: - Instrucción realizada: <%s>", query_id_actual, operacion);
			free(operacion);
			pthread_mutex_lock(&mutex_interrupcion);
			checkear_interrupcion(i);
			pthread_mutex_unlock(&mutex_interrupcion);
			break;
		} else {
			log_error_seguro(worker_logger, "Instrucción desconocida: %s", operacion);
		}
		log_info_seguro(worker_logger,"## Query <%d>: - Instrucción realizada: <%s>", query_id_actual, operacion);
		free(operacion);

		pthread_mutex_lock(&mutex_interrupcion);
		bool hubo_interrupcion= checkear_interrupcion(i);
		pthread_mutex_unlock(&mutex_interrupcion);
		if(hubo_interrupcion) break;
	}
}

bool ejecutar_create(char* instruccion) {

	bool valor_retorno;
	char* file_tag = leer_palabra(instruccion, 1);
	if (file_tag == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "CREATE: Formato inválido: %s", instruccion);
		free(file_tag);
		return false;
	}
	log_debug_seguro(worker_logger, "## ESTOY EJECUTANDO CREATE");
	char** partes = string_split(file_tag, ":");
	char* nombre_file = strdup(partes[0]);
	char* tag = strdup(partes[1]);

	t_buffer* buffer= crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, nombre_file);
	agregar_string_a_buffer(buffer, tag);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_CREATE, buffer);
	log_debug_seguro(worker_logger, "## cod_op:%d, query_id:%d, nombre_file:%s, tag:%s", OP_CREATE, query_id_actual, nombre_file, tag);
	pthread_mutex_lock(&mutex_fd_storage);
	enviar_paquete(paquete, fd_storage);
	liberar_paquete(paquete);
	int cod_respuesta = recibir_operacion(fd_storage);
	t_buffer* buffer_respuesta= recibir_buffer(fd_storage);
	log_debug_seguro(worker_logger,"## respuesta recibida: %d", cod_respuesta);
	char* resultado = extraer_string(buffer_respuesta);

	if (strcmp(resultado, "OPERACION EXITOSA") == 0) {
		log_debug_seguro(worker_logger, "##%d - OPERACION EXITOSA: File Creado %s:%s",
		query_id_actual, nombre_file, tag);
		valor_retorno = true;
	} else {
		log_error_seguro(worker_logger,
		"##%d - Error al crear File %s:%s. Motivo: %s",
		query_id_actual, nombre_file, tag, resultado);

		// mando al master el codigo de operacion END y el motivo de error
		t_buffer* buffer_end = crear_buffer();
		agregar_int_a_buffer(buffer_end, query_id_actual);
		agregar_string_a_buffer(buffer_end, resultado);
		t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
		enviar_paquete(paquete_end, fd_master);
		liberar_paquete(paquete_end);
		valor_retorno = false;
	}

	end_enviado = true; // Mark end_enviado as true when OP_END is sent
	pthread_mutex_unlock(&mutex_fd_storage);
	liberar_buffer(buffer_respuesta);
	free(resultado);
	string_array_destroy(partes);
	free(file_tag);
	free(nombre_file);
	free(tag);
	return valor_retorno;
}


bool ejecutar_truncate(char* instruccion) {
	bool valor_retorno;
	char* file_tag = leer_palabra(instruccion, 1);
	char* tamanio_str = leer_palabra(instruccion, 2);

	if (file_tag == NULL || tamanio_str == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "TRUNCATE: Formato inválido: %s", instruccion);
		free(file_tag);
		free(tamanio_str);
		return false;
	}

	int tamanio = atoi(tamanio_str);
	char** partes = string_split(file_tag, ":");
	char* nombre_file = strdup(partes[0]);
	char* tag = strdup(partes[1]);

	t_buffer* buffer= crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, nombre_file);
	agregar_string_a_buffer(buffer, tag);
	agregar_int_a_buffer(buffer,tamanio);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_TRUNCATE, buffer);
	pthread_mutex_lock(&mutex_fd_storage);
	enviar_paquete(paquete, fd_storage);
	liberar_paquete(paquete);

	int cod_respuesta = recibir_operacion(fd_storage);
	log_debug_seguro(worker_logger,"codigo recibido:%d", cod_respuesta);
	if (cod_respuesta == RESPUESTA_RESULTADO) {
		t_buffer* buffer_respuesta=recibir_buffer(fd_storage);
		char* resultado = extraer_string(buffer_respuesta);

		if (strcmp(resultado, "OPERACION EXITOSA") == 0) {
		log_debug_seguro(worker_logger,
		"##%d - OPERACION EXITOSA: File Truncado %s:%s - Tamaño: %d",
		query_id_actual, nombre_file, tag, tamanio);
		valor_retorno = true;

		} else {
		log_error_seguro(worker_logger,
		"##%d - Error al truncar File %s:%s. Motivo: %s",
		query_id_actual, nombre_file, tag, resultado);

		// enviar OP_END al Master con motivo de error
		t_buffer* buffer_end = crear_buffer();
		agregar_int_a_buffer(buffer_end, query_id_actual);
		agregar_string_a_buffer(buffer_end, resultado);
		t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
		enviar_paquete(paquete_end, fd_master);
		liberar_paquete(paquete_end);
		valor_retorno = false;
	}
		pthread_mutex_unlock(&mutex_fd_storage);
		liberar_buffer(buffer_respuesta);
		free(resultado);
		string_array_destroy(partes);
		free(file_tag);
		free(tamanio_str);
		free(nombre_file);
		free(tag);
		return valor_retorno;
	}
	pthread_mutex_unlock(&mutex_fd_storage);
	string_array_destroy(partes);
	free(file_tag);
	free(tamanio_str);
	free(nombre_file);
	free(tag);
	return false;
}

bool ejecutar_write(char* instruccion) {

	log_debug_seguro(worker_logger, "## Ejecutando write");
	char* file_tag = leer_palabra(instruccion, 1);
	char* direccion_str = leer_palabra(instruccion, 2);
	char* contenido = leer_palabra(instruccion, 3);

	if (file_tag == NULL || direccion_str == NULL || contenido == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "WRITE: Formato inválido: %s", instruccion);
		free(file_tag); free(direccion_str); free(contenido);
		return false;
	}

	int direccion = atoi(direccion_str);
	int numero_pagina = direccion / block_size;
	int offset = direccion % block_size;
	log_debug_seguro(worker_logger, "## logro traduccir las cosas, direccion: %d, numero_pagina: %d, offset: %d", direccion, numero_pagina, offset);
	// Buscar tabla y página
	t_list* tabla = dictionary_get(tablas_de_paginas, file_tag);
	t_pagina* pagina = NULL;
	log_debug_seguro(worker_logger,"ya busque la tabla");

	if (tabla != NULL) {
		for (int i = 0; i < list_size(tabla); i++) {
			t_pagina* p = list_get(tabla, i);
			if (p->numero_pagina == numero_pagina) {
				pagina = p;
				break;
			}
		}
	}
	log_debug_seguro(worker_logger,"ya busque a ver si estaba en la tabla de paginas");
	// Si no está presente, cargar desde Storage
	if (pagina == NULL || !pagina->presente) {
		cargar_pagina(query_id_actual, file_tag, numero_pagina);
		pthread_mutex_lock(&mutex_resultado_global_lectura_bloque);
		if(respuesta_global_de_lectura_bloque.resultado!=1){
			//enviar al master que se finalizo la query por motivo:respuesta_global_de_lectura_bloque.mensaje
			//TODO AGREGAR MENSAJE PARA QUE EL WORKER DIGA ALGO
			log_error_seguro(worker_logger,"fallo al carga pagina para escribir. contenido del error:%s", respuesta_global_de_lectura_bloque.contenido);
			t_buffer* buffer_end = crear_buffer();
			agregar_int_a_buffer(buffer_end, query_id_actual);
			agregar_string_a_buffer(buffer_end, respuesta_global_de_lectura_bloque.contenido);
			t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
			enviar_paquete(paquete_end, fd_master);
			liberar_paquete(paquete_end);
			pthread_mutex_unlock(&mutex_resultado_global_lectura_bloque);
			free(file_tag);
			free(direccion_str);
			free(contenido);
			return false;
		}
		pthread_mutex_unlock(&mutex_resultado_global_lectura_bloque);
		tabla = dictionary_get(tablas_de_paginas, file_tag);
		for (int i = 0; i < list_size(tabla); i++) {
			t_pagina* p = list_get(tabla, i);
			if (p->numero_pagina == numero_pagina) {
				pagina = p;
				break;
			}
		}
	}

	if (pagina == NULL || !pagina->presente) {
		log_error_seguro(worker_logger, "WRITE: No se pudo cargar la página %d de %s", numero_pagina, file_tag);
		free(file_tag); free(direccion_str); free(contenido);
		return false;
	}
	int len = strlen(contenido);
	if(len<=block_size-offset){//si el contenido entra en la pagina contando desde el offset
		escribir_memoria(query_id_actual, pagina->marco, offset, contenido);
		pagina->modificada = true;
		pagina->timestamp = get_timestamp();
		pagina->uso = true;

	}
	else{
		int escrito = 0;

		while (escrito < len) {
			int espacio = block_size - offset;
			int a_escribir = (len - escrito < espacio) ? (len - escrito) : espacio;

			char* fragmento = strndup(contenido + escrito, a_escribir);
			escribir_memoria(query_id_actual, pagina->marco, offset, fragmento);
			free(fragmento);

			pagina->modificada = true;
			pagina->timestamp = get_timestamp();
			pagina->uso = true;

			escrito += a_escribir;

			if (escrito < len) {
				// Pasar a la siguiente página
				numero_pagina++;
				offset = 0;

				// Buscar o cargar la siguiente página
				pagina = NULL;
				for (int i = 0; i < list_size(tabla); i++) {
					t_pagina* p = list_get(tabla, i);
					if (p->numero_pagina == numero_pagina) {
						pagina = p;
						break;
					}
				}
				if (pagina == NULL || !pagina->presente) {
					cargar_pagina(query_id_actual, file_tag, numero_pagina);
					tabla = dictionary_get(tablas_de_paginas, file_tag);
					for (int i = 0; i < list_size(tabla); i++) {
						t_pagina* p = list_get(tabla, i);
						if (p->numero_pagina == numero_pagina) {
							pagina = p;
							break;
						}
					}
				}
				if (pagina == NULL || !pagina->presente) {
					log_error_seguro(worker_logger, "WRITE: No se pudo cargar la página %d de %s", numero_pagina, file_tag);
					break;
				}
			}
		}

	}

	// escribir_memoria(query_id_actual, pagina->marco, offset, contenido);
	// pagina->modificada = true;
	// pagina->timestamp = get_timestamp();
	// pagina->uso = true;

	// Separar file y tag para log
	char* file_tag_dup = strdup(file_tag);
	char* file = strtok(file_tag_dup, ":");
	char* tag = strtok(NULL, ":");

	log_debug_seguro(worker_logger, "##%d - Escritura realizada: File %s:%s - Dirección: %d - Contenido: %s",
					query_id_actual, file, tag, direccion, contenido);

	free(file_tag_dup);
	free(file_tag);
	free(direccion_str);
	free(contenido);
	return true;
}

bool ejecutar_read(char* instruccion) {
	log_debug_seguro(worker_logger, "ejecutando read");
	char* file_tag = leer_palabra(instruccion, 1);
	char* direccion_str = leer_palabra(instruccion, 2);
	char* tamanio_str = leer_palabra(instruccion, 3);

	if (file_tag == NULL || direccion_str == NULL || tamanio_str == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "READ: Formato inválido: %s", instruccion);
		free(file_tag); free(direccion_str); free(tamanio_str);
		//todo agregar mensaje de fallo para el master
		return false;
	}

	int direccion = atoi(direccion_str);
	int tamanio = atoi(tamanio_str);

	int bytes_restantes = tamanio;
	int direccion_actual = direccion;
	int bytes_leidos = 0;

	char* resultado_final = malloc(tamanio + 1);
	if (resultado_final == NULL) {
		log_error_seguro(worker_logger, "READ: Error al reservar memoria para resultado");
		free(file_tag); free(direccion_str); free(tamanio_str);
		//TODO: enviar mensaje a master
		return false;
	}

	while (bytes_restantes > 0) {
		int numero_pagina = direccion_actual / block_size;
		int offset = direccion_actual % block_size;

		// Buscar tabla de páginas
		t_pagina* pagina = buscar_pagina(file_tag, numero_pagina);

		// Si no está presente, cargar desde Storage
		if (pagina == NULL || !pagina->presente) {
			cargar_pagina(query_id_actual, file_tag, numero_pagina);
			//CHECKEAR SI VARIBLE GLOBAL TIENE RESULTADO 0=FALLO PARA ASI FINALIZAR LA QUERY Y MANDAR MENSAJE A MASTER
			pthread_mutex_lock(&mutex_resultado_global_lectura_bloque);
			if(respuesta_global_de_lectura_bloque.resultado!=1){
			//enviar al master que se finalizo la query por motivo:respuesta_global_de_lectura_bloque.mensaje
			t_buffer* buffer_end = crear_buffer();
			agregar_int_a_buffer(buffer_end, query_id_actual);
			agregar_string_a_buffer(buffer_end, respuesta_global_de_lectura_bloque.contenido);
			t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
			enviar_paquete(paquete_end, fd_master);
			liberar_paquete(paquete_end);
			pthread_mutex_unlock(&mutex_resultado_global_lectura_bloque);
			free(resultado_final);
			free(file_tag); free(direccion_str); free(tamanio_str);
			return false;
		}
		pthread_mutex_unlock(&mutex_resultado_global_lectura_bloque);
			pagina = buscar_pagina(file_tag, numero_pagina);
		}

		if (pagina == NULL || !pagina->presente) {
			log_error_seguro(worker_logger, "READ: No se pudo cargar la página %d de %s", numero_pagina, file_tag);
			free(resultado_final);
			free(file_tag); free(direccion_str); free(tamanio_str);
			return false;
		}

		int bytes_a_leer = (bytes_restantes < (block_size - offset)) ? bytes_restantes : (block_size - offset);

		char* fragmento = leer_memoria(query_id_actual, pagina->marco, offset, bytes_a_leer);
		if (fragmento == NULL) {
			log_error_seguro(worker_logger, "READ: Error al leer memoria en página %d", numero_pagina);
			free(resultado_final);
			free(file_tag); free(direccion_str); free(tamanio_str);
			return false;
		}

		memcpy(resultado_final + bytes_leidos, fragmento, bytes_a_leer);
		free(fragmento);

		pagina->timestamp = get_timestamp();
		pagina->uso = true;

		bytes_leidos += bytes_a_leer;
		direccion_actual += bytes_a_leer;
		bytes_restantes -= bytes_a_leer;
	}

	resultado_final[tamanio] = '\0';

	log_debug_seguro(worker_logger, "## Lectura realizada: File %s, contenido: %s", file_tag, resultado_final);

	// Enviar resultado al Master
	t_buffer* buffer = crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, file_tag);
	agregar_string_a_buffer(buffer, resultado_final);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_LECTURA_REALIZADA, buffer);
	enviar_paquete(paquete, fd_master);
	liberar_paquete(paquete);

	free(resultado_final);
	free(file_tag); free(direccion_str); free(tamanio_str);
	return true;
}

bool ejecutar_tag(char* instruccion) {
	bool valor_retorno;
	char* origen = leer_palabra(instruccion, 1);
	char* destino = leer_palabra(instruccion, 2);

	if (origen == NULL || destino == NULL || !string_contains(origen, ":") || !string_contains(destino, ":")) {
		log_error_seguro(worker_logger, "TAG: Formato inválido: %s", instruccion);
		free(origen); free(destino);
		return false;
	}

	char** partes_origen = string_split(origen, ":");
	char** partes_destino = string_split(destino, ":");

	char* file_origen = strdup(partes_origen[0]);
	char* tag_origen = strdup(partes_origen[1]);
	char* file_destino = strdup(partes_destino[0]);
	char* tag_destino = strdup(partes_destino[1]);


	 t_buffer* buffer= crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, file_origen);
	agregar_string_a_buffer(buffer, tag_origen);
	agregar_string_a_buffer(buffer, file_destino);
	agregar_string_a_buffer(buffer, tag_destino);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_TAG, buffer);
	pthread_mutex_lock(&mutex_fd_storage);
	enviar_paquete(paquete, fd_storage);
	liberar_paquete(paquete);

	int cod_respuesta = recibir_operacion(fd_storage);
	if (cod_respuesta == RESPUESTA_RESULTADO) {
		t_buffer* buffer_respuesta = recibir_buffer(fd_storage);
		char* resultado = extraer_string(buffer_respuesta);
		// liberar el buffer recibido para evitar fuga
		liberar_buffer(buffer_respuesta);

		if (strcmp(resultado, "OPERACION EXITOSA") == 0) {
			log_debug_seguro(worker_logger,"##%d - OPERACION EXITOSA: Tag creado %s:%s", query_id_actual, file_destino, tag_destino);
			valor_retorno = true;

		} else {
			log_error_seguro(worker_logger,
			"##%d - Error al crear Tag %s:%s. Motivo: %s",
			query_id_actual, file_destino, tag_destino, resultado);

			// enviar OP_END al Master con motivo de error
			t_buffer* buffer_end = crear_buffer();
			agregar_int_a_buffer(buffer_end, query_id_actual);
			agregar_string_a_buffer(buffer_end, resultado);
			t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
			enviar_paquete(paquete_end, fd_master);
			liberar_paquete(paquete_end);
			valor_retorno = false;
		}
		free(resultado);
	} else {
		log_error_seguro(worker_logger,
		"##%d - Error al crear Tag %s:%s",
		query_id_actual, file_destino, tag_destino);

		// enviar OP_END al Master con motivo de error genérico
		t_buffer* buffer_end = crear_buffer();
		agregar_int_a_buffer(buffer_end, query_id_actual);
		agregar_string_a_buffer(buffer_end, "ERROR: respuesta inválida de Storage");
		t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
		enviar_paquete(paquete_end, fd_master);
		liberar_paquete(paquete_end);
		valor_retorno = false;
	}

	pthread_mutex_unlock(&mutex_fd_storage);

	string_array_destroy(partes_origen);
	string_array_destroy(partes_destino);
	free(origen); free(destino);
	free(file_origen); free(tag_origen); free(file_destino); free(tag_destino);
	return valor_retorno;
}

bool ejecutar_commit(char* instruccion) {
	bool valor_retorno = false;
	char* file_tag = leer_palabra(instruccion, 1);
	if (file_tag == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "COMMIT: Formato inválido: %s", instruccion);
		free(file_tag);
		return false;
	}

	char** partes = string_split(file_tag, ":");
	char* nombre_file = strdup(partes[0]);
	char* tag = strdup(partes[1]);

	pthread_mutex_lock(&mutex_tablas);
	t_list* tabla = dictionary_get(tablas_de_paginas, file_tag);

	if (tabla != NULL) {
		for (int i = 0; i < list_size(tabla); i++){
			t_pagina* pagina = list_get(tabla, i);
			if (pagina->modificada) {
				bool resultado = flush_pagina(file_tag, pagina, query_id_actual);
				if (!resultado) {
					pthread_mutex_unlock(&mutex_tablas);
					string_array_destroy(partes);
					free(file_tag); free(nombre_file); free(tag);
					return false;
				}
			}
		}
	} else {
		log_debug_seguro(worker_logger, "COMMIT: No hay páginas en memoria para %s", file_tag);
	}
	pthread_mutex_unlock(&mutex_tablas);

	t_buffer* buffer = crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, nombre_file);
	agregar_string_a_buffer(buffer, tag);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_COMMIT, buffer);

	pthread_mutex_lock(&mutex_fd_storage);
	enviar_paquete(paquete, fd_storage);
	liberar_paquete(paquete);

	int cod_respuesta = recibir_operacion(fd_storage);
	if (cod_respuesta == RESPUESTA_RESULTADO) {
		t_buffer* buffer_respuesta = recibir_buffer(fd_storage);
		char* resultado = extraer_string(buffer_respuesta);

		if (strcmp(resultado, "OPERACION EXITOSA") == 0) {
			log_debug_seguro(worker_logger, "##%d - OPERACION EXITOSA: Commit de File:Tag %s:%s", query_id_actual, nombre_file, tag);
			valor_retorno = true;
		} else {
			log_error_seguro(worker_logger,
				"##%d - Error al hacer Commit de %s:%s. Motivo: %s",
				query_id_actual, nombre_file, tag, resultado);

			t_buffer* buffer_end = crear_buffer();
			agregar_int_a_buffer(buffer_end, query_id_actual);
			agregar_string_a_buffer(buffer_end, resultado);
			t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
			enviar_paquete(paquete_end, fd_master);
			liberar_paquete(paquete_end);
			valor_retorno = false;
		}

		liberar_buffer(buffer_respuesta);
		free(resultado);
	} else {
		log_error_seguro(worker_logger,
			"##%d - Error al hacer Commit de %s:%s",
			query_id_actual, nombre_file, tag);

		t_buffer* buffer_end = crear_buffer();
		agregar_int_a_buffer(buffer_end, query_id_actual);
		agregar_string_a_buffer(buffer_end, "ERROR: respuesta inválida de Storage");
		t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
		enviar_paquete(paquete_end, fd_master);
		liberar_paquete(paquete_end);
		valor_retorno = false;
	}
	pthread_mutex_unlock(&mutex_fd_storage);

	string_array_destroy(partes);
	free(file_tag); free(nombre_file); free(tag);
	return valor_retorno;
}

bool ejecutar_flush(char* instruccion) {
	bool valor_retorno;
	log_debug_seguro(worker_logger, "vamos a flushear");
	char* file_tag = leer_palabra(instruccion, 1);
	if (file_tag == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "FLUSH: Formato inválido: %s", instruccion);
		free(file_tag);
		return false;
	}
	//  log_debug_seguro(worker_logger, "proce");
	// char** partes = string_split(file_tag, ":");
	// char* nombre_file = strdup(partes[0]);
	// char* tag = strdup(partes[1]);

	pthread_mutex_lock(&mutex_tablas);
	t_list* tabla = dictionary_get(tablas_de_paginas, file_tag);

	// void aplicar_flush(void* value) {
	//     t_pagina* pagina = (t_pagina*)value;
	//     if (pagina->modificada) flush_pagina(file_tag, pagina, query_id_actual);
	// }

	log_debug_seguro(worker_logger,"ya tengo la lista para iterar");
	if (tabla != NULL) {
		//list_iterate(tabla, aplicar_flush);
		 for (int i=0; i<list_size(tabla); i++){
			t_pagina* pagina = list_get(tabla,i);
			if (pagina->modificada) {
				bool resultado = flush_pagina(file_tag, pagina, query_id_actual);
				if(!resultado) {
				pthread_mutex_unlock(&mutex_tablas);

				log_debug_seguro(worker_logger, "flushee lo que habia");

				free(file_tag);
				return false;}
			}
		}
		valor_retorno=true;
	} else {
		log_debug_seguro(worker_logger, "FLUSH: No hay páginas en memoria para %s", file_tag);
		valor_retorno = false;
	}
	pthread_mutex_unlock(&mutex_tablas);

	log_debug_seguro(worker_logger, "flushee lo que habia");

	free(file_tag);// free(nombre_file); free(tag);
	return valor_retorno;
}

bool ejecutar_delete(char* instruccion) {
	bool valor_retorno;
	char* file_tag = leer_palabra(instruccion, 1);
	if (file_tag == NULL || !string_contains(file_tag, ":")) {
		log_error_seguro(worker_logger, "DELETE: Formato inválido: %s", instruccion);
		free(file_tag);
		return false;
	}

	char** partes = string_split(file_tag, ":");
	char* nombre_file = strdup(partes[0]);
	char* tag = strdup(partes[1]);

	t_buffer* buffer= crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, nombre_file);
	agregar_string_a_buffer(buffer, tag);
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_DELETE, buffer);
	pthread_mutex_lock(&mutex_fd_storage);
	enviar_paquete(paquete, fd_storage);
	liberar_paquete(paquete);

	int cod_respuesta = recibir_operacion(fd_storage);

	if (cod_respuesta == RESPUESTA_RESULTADO) {
		t_buffer* buffer_respuesta = recibir_buffer(fd_storage);
		char* resultado = extraer_string(buffer_respuesta);

		/* Liberamos el buffer y el mutex de fd_storage antes de modificar tablas */
		liberar_buffer(buffer_respuesta);
		pthread_mutex_unlock(&mutex_fd_storage);

		if (strcmp(resultado, "OPERACION EXITOSA") == 0) {
			log_debug_seguro(worker_logger, "##%d - OPERACION EXITOSA: Tag Eliminado %s:%s",
							query_id_actual, nombre_file, tag);

			/* Si el DELETE fue exitoso, remover y liberar la tabla de páginas asociada a ese File:Tag */
			pthread_mutex_lock(&mutex_tablas);
			t_list* claves = dictionary_keys(tablas_de_paginas);
			char* clave_encontrada = NULL;
			for (int i = 0; i < list_size(claves); i++) {
				char* k = list_get(claves, i);
				if (strcmp(k, file_tag) == 0) { clave_encontrada = k; break; }
			}
			list_destroy(claves);

			if (clave_encontrada != NULL) {
				t_list* tabla = dictionary_remove(tablas_de_paginas, clave_encontrada);
				if (tabla != NULL) {
					list_destroy_and_destroy_elements(tabla, liberar_pagina);
				}
			}
			pthread_mutex_unlock(&mutex_tablas);

			valor_retorno = true;

		} else {
			log_error_seguro(worker_logger, "##%d - Error al eliminar Tag %s:%s. Motivo: %s", query_id_actual, nombre_file, tag, resultado);
			t_buffer* buffer_end = crear_buffer();
			agregar_int_a_buffer(buffer_end, query_id_actual);
			agregar_string_a_buffer(buffer_end, resultado);
			t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
			enviar_paquete(paquete_end, fd_master);
			liberar_paquete(paquete_end);
			valor_retorno = false;
		}

		free(resultado);

	} else {
		log_error_seguro(worker_logger,
			"##%d - Error al eliminar Tag %s:%s",
			query_id_actual, nombre_file, tag);

		/* Liberar mutex_fd_storage antes de tocar otras estructuras */
		pthread_mutex_unlock(&mutex_fd_storage);

		t_buffer* buffer_end = crear_buffer();
		agregar_int_a_buffer(buffer_end, query_id_actual);
		agregar_string_a_buffer(buffer_end, "ERROR: respuesta inválida de Storage");
		t_paquete* paquete_end = crear_paquete_con_codigo_de_operacion(OP_END, buffer_end);
		enviar_paquete(paquete_end, fd_master);
		liberar_paquete(paquete_end);
		valor_retorno = false;
	}

	string_array_destroy(partes);
	free(file_tag); free(nombre_file); free(tag);
	return valor_retorno;
}

bool ejecutar_end(char* instruccion) {
	t_buffer* buffer = crear_buffer();
	agregar_int_a_buffer(buffer, query_id_actual);
	agregar_string_a_buffer(buffer, "FINALIZACION NORMAL");
	t_paquete* paquete = crear_paquete_con_codigo_de_operacion(OP_END, buffer);
	enviar_paquete(paquete, fd_master); // Se envía al Master
	liberar_paquete(paquete);

	log_debug_seguro(worker_logger, "## Query Finalizada - Motivo: END");
	return true;

}

void* ejecutar_query(void* args){
	t_query* query = (t_query*) args;
	log_debug_seguro(worker_logger,"archivo a ejecutar:%s", query->archivo_query);
	t_list* instrucciones = obtener_instrucciones_proceso(query->archivo_query);
	query_id_actual=query->id_query;
	interpretar_instrucciones(instrucciones, query->pc_inicial);
	list_destroy_and_destroy_elements(instrucciones, free);

	// Liberar la estructura de argumentos y el nombre del archivo que el hilo recibió
	if (query->archivo_query) free(query->archivo_query);
	free(query);

	return NULL;

}

bool checkear_interrupcion(int pc_actual){

	if(interrupcion.hay_interrupcion && interrupcion.query_id>-1 && interrupcion.query_id==query_id_actual){//ver si es necesario proteger query_id_actual. sino recomiendo pasar la query actual como parametro tanto para esta funcion como para la funcion de interpretar instrucciones
		log_info_seguro(worker_logger,  "## Query <%d>: Desalojada por pedido del Master", query_id_actual);
		int pc=pc_actual+1; //para guardar el siguiente pc a ejecutar de la query
		enviar_respuesta_interrupcion(fd_master, pc, interrupcion.query_id);
		interrupcion.query_id = -1;
		interrupcion.hay_interrupcion=false;
		return true;
	}
	else return false;
}