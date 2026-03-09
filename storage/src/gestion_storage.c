#include "../includes/gestion_storage.h"

int reservar_bloque_fisico_libre() {
    pthread_mutex_lock(&mutex_bitmap);

    int cantidad_bloques = FS_SIZE / BLOCK_SIZE;
    for (int i = 0; i < cantidad_bloques; i++) {
        if (!bitarray_test_bit(bitmap, i)) {
            bitarray_set_bit(bitmap, i);
            msync(bitmap_mem, cantidad_bloques / 8, MS_SYNC);
            pthread_mutex_unlock(&mutex_bitmap);

            log_info_seguro(storage_logger, "Bloque Físico Reservado - Numero de Bloque: <%d>", i);
            return i;
        }
    }

    pthread_mutex_unlock(&mutex_bitmap);
    log_error_seguro(storage_logger, "No hay bloques físicos libres disponibles");
    return -1;
}

void liberar_bloque_fisico(int bloque) {
    pthread_mutex_lock(&mutex_bitmap);
    bitarray_clean_bit(bitmap, bloque);
    msync(bitmap_mem, FS_SIZE / 8, MS_SYNC);
    pthread_mutex_unlock(&mutex_bitmap);

    char path_fisico[PATH_MAX];
    if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path truncado en liberar_bloque_fisico");
        return;
    }

    int fd = open(path_fisico, O_WRONLY | O_TRUNC);
    if (fd != -1) {
        ftruncate(fd, BLOCK_SIZE);
        void* bloque_mem = mmap(NULL, BLOCK_SIZE, PROT_WRITE, MAP_SHARED, fd, 0);
        if (bloque_mem != MAP_FAILED) {
            memset(bloque_mem, '0', BLOCK_SIZE);
            msync(bloque_mem, BLOCK_SIZE, MS_SYNC);
            munmap(bloque_mem, BLOCK_SIZE);
        }
        close(fd);
    }

    log_info_seguro(storage_logger, "Bloque físico liberado - Numero de Bloque: <%d>", bloque);
}
int contar_referencias_bloque(int bloque_fisico) {
    int referencias = 0;

    DIR* dir_files = opendir(path_files);
    if (!dir_files) return 0;

    struct dirent* file_entry;
    while ((file_entry = readdir(dir_files)) != NULL) {
        if (file_entry->d_type != DT_DIR || string_starts_with(file_entry->d_name, ".")) continue;

        char path_file[PATH_MAX];
        if (snprintf(path_file, PATH_MAX, "%s/%s", path_files, file_entry->d_name) >= PATH_MAX) {
            log_error_seguro(storage_logger, "Path truncado en path_file");
            continue;
        }

        DIR* dir_tags = opendir(path_file);
        if (!dir_tags) continue;

        struct dirent* tag_entry;
        while ((tag_entry = readdir(dir_tags)) != NULL) {
            if (tag_entry->d_type != DT_DIR || string_starts_with(tag_entry->d_name, ".")) continue;

            char path_logical_blocks[PATH_MAX];
            if (snprintf(path_logical_blocks, PATH_MAX, "%s/%s/logical_blocks", path_file, tag_entry->d_name) >= PATH_MAX) {
                log_error_seguro(storage_logger, "Path truncado en path_logical_blocks");
                continue;
            }

            DIR* dir_blocks = opendir(path_logical_blocks);
            if (!dir_blocks) continue;

            struct dirent* block_entry;
            while ((block_entry = readdir(dir_blocks)) != NULL) {
                if (block_entry->d_type != DT_REG || string_starts_with(block_entry->d_name, ".")) continue;

                char path_logico[PATH_MAX];
                if (snprintf(path_logico, PATH_MAX, "%s/%s", path_logical_blocks, block_entry->d_name) >= PATH_MAX) {
                    log_error_seguro(storage_logger, "Path truncado en path_logico");
                    continue;
                }

                struct stat stat_logico;
                if (stat(path_logico, &stat_logico) == -1) continue;

                char path_fisico[PATH_MAX];
                if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico) >= PATH_MAX) {
                    log_error_seguro(storage_logger, "Path truncado en path_fisico");
                    continue;
                }

                struct stat stat_fisico;
                if (stat(path_fisico, &stat_fisico) == -1) continue;

                if (stat_logico.st_ino == stat_fisico.st_ino) {
                    referencias++;
                }
            }

            closedir(dir_blocks);
        }

        closedir(dir_tags);
    }

    closedir(dir_files);
    return referencias;
}

void aplicar_retardo_op() {
    if (RETARDO_OPERACION > 0) {
        usleep(RETARDO_OPERACION * 1000);
    }
}

void aplicar_retardo_block() {
    if (RETARDO_ACCESO_BLOQUE > 0) {
        usleep(RETARDO_ACCESO_BLOQUE * 1000);
    }
}


char* crear_file_tag(const char* nombre_file, const char* nombre_tag) {//CHAR*
    aplicar_retardo_op();

    char path_file[PATH_MAX];
    char path_tag[PATH_MAX];
    char path_metadata[PATH_MAX];
    char path_logical_blocks[PATH_MAX];

    // Construcción de paths con chequeos de truncado
    if (snprintf(path_file, PATH_MAX, "%s/%s", path_files, nombre_file) >= PATH_MAX ||
        snprintf(path_tag, PATH_MAX, "%s/%s", path_file, nombre_tag) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path truncado en CREATE de %s:%s", nombre_file, nombre_tag);
        return "PATH TRUNCADO EN CREATE";
    }

    // Bloqueo granular por File:Tag (sin mutex global)
    lock_tag(nombre_file, nombre_tag);

    // Verificar si ya existe el File:Tag
    if (access(path_tag, F_OK) != -1) {
        log_error_seguro(storage_logger, "CREATE falló: File:Tag ya existe (%s:%s)", nombre_file, nombre_tag);
        unlock_tag(nombre_file, nombre_tag);
        return "FILE/TAG_EXISTENTE";//"FILE_EXISTENTE"
    }

    // Crear directorio del File (idempotente)
    if (mkdir(path_file, 0777) == -1 && errno != EEXIST) {
        perror("Error creando directorio del File");
        unlock_tag(nombre_file, nombre_tag);
        return "FALLO CREANDO DIRECTORIO DEL FILE";
    }

    // Crear directorio del Tag
    if (mkdir(path_tag, 0777) == -1) {
        perror("Error creando directorio del Tag");
        unlock_tag(nombre_file, nombre_tag);
        return "FALLO CREANDO DIRECTORIO DEL TAG";
    }

    // Crear directorio logical_blocks
    if (mkdir(path_logical_blocks, 0777) == -1) {
        perror("Error creando directorio logical_blocks");
        unlock_tag(nombre_file, nombre_tag);
        return "FALLO CREANDO DIRECTORIO LOGICAL BLOCKS";
    }

    // Crear metadata.config inicial
    FILE* metadata = fopen(path_metadata, "w");
    if (!metadata) {
        perror("Error creando metadata.config en CREATE");
        unlock_tag(nombre_file, nombre_tag);
        return "FALLO CREANDO METADATA";
    }
    fprintf(metadata, "TAMAÑO=0\nBLOCKS=[]\nESTADO=WORK_IN_PROGRESS\n");
    fclose(metadata);

    //Desbloqueo del File:Tag
    unlock_tag(nombre_file, nombre_tag);

    log_debug_seguro(storage_logger, "CREATE exitoso: %s:%s", nombre_file, nombre_tag);
    return "OPERACION EXITOSA";//OPERACION EXITOSA
}


char* truncate_file_tag(const char* nombre_file, const char* nombre_tag, int nuevo_tamanio, int query_id) {
    aplicar_retardo_op();
    if (nuevo_tamanio < 0) {
        log_error_seguro(storage_logger, "##%d - Tamaño inválido en TRUNCATE: %d", query_id, nuevo_tamanio);
        return "TAMANIO INVALIDO";
    }

    char path_tag[PATH_MAX], path_metadata[PATH_MAX], path_logical_blocks[PATH_MAX];

    if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_tag", query_id);
        return "ERROR TRUNCAMIENTO EN PATH_TAG";
    }
    if (snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_metadata", query_id);
        return "ERROR TRUNCAMIENTO EN PATH_METADATA";
    }
    if (snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_logical_blocks", query_id);
        return "ERROR TRUNCAMIENTO EN PATH_LOGICAL_BLOCKS";
    }

    // Bloqueo granular por File:Tag (sin mutex global)
    lock_tag(nombre_file, nombre_tag);

    if (access(path_metadata, F_OK) == -1) {
        log_error_seguro(storage_logger, "##%d - File:Tag inexistente en TRUNCATE (%s:%s)", query_id, nombre_file, nombre_tag);
        unlock_tag(nombre_file, nombre_tag);
        return "FILE:TAG INEXISTENTE";
    }

    t_config* metadata = config_create(path_metadata);
    if (!metadata) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en TRUNCATE", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "METADATA.CONFIG NO LEIDO";
    }

    char* estado = config_get_string_value(metadata, "ESTADO");
    if (!estado || strcmp(estado, "COMMITED") == 0) {
        log_error_seguro(storage_logger, "##%d - No se puede truncar un File:Tag COMMITED", query_id);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        return "ERROR TRUNCAMIENTO FILE:TAG COMMITED";
    }

    // Redondeo a múltiplos de BLOCK_SIZE
    if (nuevo_tamanio % BLOCK_SIZE != 0) {
        nuevo_tamanio = ((nuevo_tamanio / BLOCK_SIZE) + 1) * BLOCK_SIZE;
    }

    int tamanio_actual   = config_get_int_value(metadata, "TAMAÑO");
    int bloques_actuales = tamanio_actual   / BLOCK_SIZE;
    int bloques_nuevos   = nuevo_tamanio    / BLOCK_SIZE;

    t_list*   lista_bloques  = list_create();
    char**    bloques_config = config_get_array_value(metadata, "BLOCKS");

    for (int i = 0; bloques_config[i] != NULL; i++) {
        // Duplicamos cada entrada para trabajar en la lista temporal
        list_add(lista_bloques, strdup(bloques_config[i]));
    }

    //Expansión de logical blocks

    if (bloques_nuevos > bloques_actuales) {
        for (int i = bloques_actuales; i < bloques_nuevos; i++) {
            // Según tu diseño, los bloques nuevos apuntan al físico 0
            int bloque_fisico = 0;
            // Si en el futuro cambiás a reservar bloques, llamá a reservar_bloque_fisico_libre()

            char* bloque_str = string_from_format("%d", bloque_fisico);
            list_add(lista_bloques, bloque_str);

            char path_logico[PATH_MAX], path_fisico[PATH_MAX];
            if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
                log_error_seguro(storage_logger, "##%d - Path truncado en path_logico (agregado)", query_id);
                continue; // dejamos el entry en la lista, aunque no se haya creado el link
            }
            if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico) >= PATH_MAX) {
                log_error_seguro(storage_logger, "##%d - Path truncado en path_fisico (agregado)", query_id);
                continue;
            }

            // Crear y (si hace falta) inicializar bloque físico
            aplicar_retardo_block();
            int fd_fisico = open(path_fisico, O_CREAT | O_RDWR, 0666);
            if (fd_fisico != -1) {
                ftruncate(fd_fisico, BLOCK_SIZE);
                void* bloque_mem = mmap(NULL, BLOCK_SIZE, PROT_WRITE, MAP_SHARED, fd_fisico, 0);
                if (bloque_mem != MAP_FAILED) {
                    memset(bloque_mem, '0', BLOCK_SIZE);
                    msync(bloque_mem, BLOCK_SIZE, MS_SYNC);
                    munmap(bloque_mem, BLOCK_SIZE);
                }
                close(fd_fisico);
            }

            // Crear hard link lógico
            aplicar_retardo_block();
            if (link(path_fisico, path_logico) == -1) {
                perror("Error creando hard link en TRUNCATE");
                continue;
            }

            log_info_seguro(storage_logger,
                            "##<%d> - <%s>:<%s> Se agregó el hard link del bloque lógico <%d> al bloque físico <%d>",
                            query_id, nombre_file, nombre_tag, i, bloque_fisico);
        }
    }

    //Reducción de logical blocks

    if (bloques_nuevos < bloques_actuales) {
        for (int i = bloques_actuales - 1; i >= bloques_nuevos; i--) {
            char* bloque_str   = list_get(lista_bloques, i);
            int   bloque_fisico = atoi(bloque_str);

            char path_logico[PATH_MAX];
            if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
                log_error_seguro(storage_logger, "##%d - Path truncado en path_logico (eliminado)", query_id);
                continue;
            }

            aplicar_retardo_block();
            unlink(path_logico);
            log_info_seguro(storage_logger,
                            "##<%d> - <%s>:<%s> Se eliminó el hard link del bloque lógico <%d> al bloque físico <%d>",
                            query_id, nombre_file, nombre_tag, i, bloque_fisico);

            // Si ya no hay referencias al físico, liberarlo (liberar_bloque_fisico bloquea el bitmap internamente)
            if (contar_referencias_bloque(bloque_fisico) == 0) {
                aplicar_retardo_block();
                liberar_bloque_fisico(bloque_fisico);
                log_info_seguro(storage_logger,
                                "##<%d> - Bloque Físico Liberado - Número de Bloque: <%d>", query_id, bloque_fisico);
            }

            free(bloque_str);
            list_remove(lista_bloques, i);
        }
    }

    //Actualizar metadata.config

    FILE* f_meta = fopen(path_metadata, "w");
    if (!f_meta) {
        log_error_seguro(storage_logger, "##%d - No se pudo abrir metadata.config para escritura en TRUNCATE", query_id);
        list_destroy_and_destroy_elements(lista_bloques, free);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        return "ERROR APERTURA METADATA.CONFIG";
    }

    fprintf(f_meta, "TAMAÑO=%d\nBLOCKS=[", nuevo_tamanio);
    for (int i = 0; i < list_size(lista_bloques); i++) {
        char* bloque = list_get(lista_bloques, i);
        fprintf(f_meta, "%s%s", i == 0 ? "" : ",", bloque);
    }
    fprintf(f_meta, "]\nESTADO=WORK_IN_PROGRESS\n");
    fclose(f_meta);

    // Limpieza
    list_destroy_and_destroy_elements(lista_bloques, free);
    string_array_destroy(bloques_config);
    config_destroy(metadata);

    // Desbloqueo del File:Tag
    unlock_tag(nombre_file, nombre_tag);

    return "OPERACION EXITOSA";
}



char* write_block(const char* nombre_file, const char* nombre_tag, int bloque_logico, const char* contenido, int query_id) {
    aplicar_retardo_op();
    log_debug_seguro(storage_logger, "##%d - strlen(contenido): %ld, BLOCK_SIZE: %d", query_id, strlen(contenido), BLOCK_SIZE);

    t_config* metadata = NULL;
    char** bloques_config = NULL;

    char path_tag[PATH_MAX];
    char path_metadata[PATH_MAX];
    char path_logical_blocks[PATH_MAX];

    if (strlen(contenido) != BLOCK_SIZE) {
        log_error_seguro(storage_logger, "##%d - Contenido inválido en WRITE: tamaño %ld, esperado %d", query_id, strlen(contenido), BLOCK_SIZE);
        return "ESCRITURA FUERA DEL LIMITE O MENOR AL TAMAÑO DEL BLOQUE";
    }

    if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en WRITE", query_id);
        return "ERROR PATH TRUNCADO EN WRITE";
    }

    /* Bloqueo granular por File:Tag: protege metadata y hard links de este recurso */
    lock_tag(nombre_file, nombre_tag);

    if (access(path_metadata, F_OK) == -1) {
        log_error_seguro(storage_logger, "##%d - File:Tag inexistente en WRITE (%s:%s)", query_id, nombre_file, nombre_tag);
        unlock_tag(nombre_file, nombre_tag);
        return "FILE:TAG INEXISTENTE EN WRITE";
    }

    metadata = config_create(path_metadata);
    if (!metadata) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en WRITE", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "NO SE PUDO LEER METADATA.CONFIG EN WRITE";
    }

    {
        char* estado = config_get_string_value(metadata, "ESTADO");
        if (!estado || strcmp(estado, "COMMITED") == 0) {
            log_error_seguro(storage_logger, "##%d - No se puede escribir en un File:Tag COMMITED", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "NO SE PUEDE ESCRIBIR EN UN FILE:TAG COMMITED";
        }
    }

    bloques_config = config_get_array_value(metadata, "BLOCKS");
    if (!bloques_config) {
        log_error_seguro(storage_logger, "##%d - Metadata sin BLOCKS en WRITE", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "METADATA SIN BLOCKS EN WRITE";
    }

    int cantidad_bloques = string_array_size(bloques_config);
    if (bloque_logico < 0 || bloque_logico >= cantidad_bloques) {
        log_error_seguro(storage_logger, "##%d - Bloque lógico fuera de rango en WRITE (%d)", query_id, bloque_logico);
        unlock_tag(nombre_file, nombre_tag);
        return "BLOQUE LOGICO FUERA DE RANGO EN WRITE";
    }

    int bloque_fisico_actual = atoi(bloques_config[bloque_logico]);

    char path_logico[PATH_MAX];
    if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, bloque_logico) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_logico", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "ERROR PATH TRUNCADO EN PATH_LOGICO";
    }

    char path_fisico[PATH_MAX];
    if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico_actual) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_fisico", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "ERROR PATH TRUNCADO EN PATH_FISICO";
    }

    int refs = contar_referencias_bloque(bloque_fisico_actual);

    if (refs > 1) {
        /* Reasignación: reservar nuevo físico (reservar_bloque_fisico_libre bloquea bitmap internamente) */
        int nuevo_bloque_fisico = reservar_bloque_fisico_libre();
        if (nuevo_bloque_fisico == -1) {
            log_error_seguro(storage_logger, "##%d - No hay bloques físicos disponibles para WRITE", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "SIN BLOQUES FISICOS DISPONIBLES PARA WRITE";
        }

        char path_nuevo_fisico[PATH_MAX];
        if (snprintf(path_nuevo_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, nuevo_bloque_fisico) >= PATH_MAX) {
            log_error_seguro(storage_logger, "##%d - Path truncado en path_nuevo_fisico", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "ERROR PATH TRUNCADO EN PATH_NUEVO_FISICO";
        }

        /* Escribir contenido en el nuevo bloque físico */
        aplicar_retardo_block();
        int fd_nuevo = open(path_nuevo_fisico, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (fd_nuevo == -1) {
            perror("Error abriendo nuevo bloque físico para escritura");
            unlock_tag(nombre_file, nombre_tag);
            return "ERROR ABRIENDO NUEVO BLOQUE FISICO PARA ESCRITURA";
        }
        write(fd_nuevo, contenido, BLOCK_SIZE);
        fsync(fd_nuevo);
        close(fd_nuevo);

        /* Reapuntar hard link del bloque lógico al nuevo físico */
        unlink(path_logico);
        link(path_nuevo_fisico, path_logico);

        /* Actualizar metadata: liberar string viejo antes de asignar el nuevo */
        {
            char* nuevo = string_from_format("%d", nuevo_bloque_fisico);
            if (nuevo) {
                free(bloques_config[bloque_logico]);
                bloques_config[bloque_logico] = nuevo;
            }
        }

        /* Intentar liberar el físico anterior si ya no tiene referencias (liberar_bloque_fisico bloquea bitmap internamente) */
        if (contar_referencias_bloque(bloque_fisico_actual) == 0) {
            aplicar_retardo_block();
            liberar_bloque_fisico(bloque_fisico_actual);
            log_info_seguro(storage_logger, "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, bloque_fisico_actual);
        }

        log_info_seguro(storage_logger, "##%d - %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d",
                        query_id, nombre_file, nombre_tag, bloque_logico, bloque_fisico_actual);
        log_info_seguro(storage_logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d",
                        query_id, nombre_file, nombre_tag, bloque_logico, nuevo_bloque_fisico);
        log_info_seguro(storage_logger, "##%d - Bloque Físico Reservado - Número de Bloque: %d", query_id, nuevo_bloque_fisico);

    } else {
        /* Escritura directa sobre el bloque físico actual */
        aplicar_retardo_block();
        int fd = open(path_fisico, O_WRONLY | O_TRUNC);
        if (fd == -1) {
            perror("Error abriendo bloque físico para escritura");
            unlock_tag(nombre_file, nombre_tag);
            return "ERROR ABRIENDO BLOQUE FISICO PARA ESCRITURA";
        }
        write(fd, contenido, BLOCK_SIZE);
        fsync(fd);
        close(fd);
    }

    /* Actualizar metadata.config (mantener estado WORK_IN_PROGRESS) */
    {
        FILE* f_meta = fopen(path_metadata, "w");
        if (!f_meta) {
            log_error_seguro(storage_logger, "##%d - No se pudo abrir metadata.config para escritura en WRITE", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "NO SE PUDO ABRIR METADATA.CONFIG PARA ESCRITURA";
        }

        int nuevo_tamanio = config_get_int_value(metadata, "TAMAÑO");
        fprintf(f_meta, "TAMAÑO=%d\nBLOCKS=[", nuevo_tamanio);
        for (int i = 0; bloques_config[i] != NULL; i++) {
            fprintf(f_meta, "%s%s", i == 0 ? "" : ",", bloques_config[i]);
        }
        fprintf(f_meta, "]\nESTADO=WORK_IN_PROGRESS\n");
        fclose(f_meta);
    }

    log_info_seguro(storage_logger, "##<%d> - Bloque Lógico Escrito <%s>:<%s> - Número de Bloque: <%d>",
                    query_id, nombre_file, nombre_tag, bloque_logico);


    if (bloques_config) string_array_destroy(bloques_config);
    if (metadata) config_destroy(metadata);
    unlock_tag(nombre_file, nombre_tag);
    return "OPERACION EXITOSA";
}



t_resultado_leer* read_block(const char* nombre_file, const char* nombre_tag, int bloque_logico, int query_id) {
    aplicar_retardo_op();

    t_resultado_leer* resultado = malloc(sizeof(t_resultado_leer));
    if (!resultado) {
        // Si querés, podés loguear y abortar; pero para mantener contrato:
        // devolver mensaje heap para que el caller pueda free() sin condicionales.
        // O retornar NULL y que el caller maneje.
        // Aquí mantengo contrato:
        resultado = NULL;
        return NULL;
    }
    resultado->resultado = 0;
    resultado->mensaje = NULL;

    char path_tag[PATH_MAX];
    char path_metadata[PATH_MAX];
    char path_logical_blocks[PATH_MAX];

    if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en READ", query_id);
        resultado->mensaje = strdup("ERROR PATH TRUNCADO EN READ");
        return resultado;
    }

    lock_tag(nombre_file, nombre_tag);

    if (access(path_metadata, F_OK) == -1) {
        log_error_seguro(storage_logger, "##%d - File:Tag inexistente en READ (%s:%s)", query_id, nombre_file, nombre_tag);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("FILE:TAG INEXISTENTE EN READ");
        return resultado;
    }

    t_config* metadata = config_create(path_metadata);
    if (!metadata) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en READ", query_id);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("NO SE PUDO LEER METADATA.CONFIG EN READ");
        return resultado;
    }

    char** bloques_config = config_get_array_value(metadata, "BLOCKS");
    if (!bloques_config) {
        log_error_seguro(storage_logger, "##%d - Metadata sin BLOCKS en READ", query_id);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("METADATA SIN BLOCKS EN READ");
        return resultado;
    }

    int cantidad_bloques = string_array_size(bloques_config);
    if (bloque_logico < 0 || bloque_logico >= cantidad_bloques) {
        log_error_seguro(storage_logger, "##%d - Bloque lógico fuera de rango en READ (%d)", query_id, bloque_logico);
        string_array_destroy(bloques_config);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("BLOQUE LOGICO FUERA DE RANGO EN READ");
        return resultado;
    }

    char path_logico[PATH_MAX];
    if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, bloque_logico) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en path_logico", query_id);
        string_array_destroy(bloques_config);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("ERROR PATH TRUNCADO EN PATH_LOGICO");
        return resultado;
    }

    FILE* f_logico = fopen(path_logico, "rb");
    if (!f_logico) {
        log_error_seguro(storage_logger, "##%d - No se pudo abrir el bloque lógico para lectura", query_id);
        string_array_destroy(bloques_config);
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        resultado->mensaje = strdup("NO SE PUDO ABRIR BLOQUE LOGICO PARA LECTURA");
        return resultado;
    }

    // Ya no necesitamos metadata ni el array de bloques.
    string_array_destroy(bloques_config);
    config_destroy(metadata);
    unlock_tag(nombre_file, nombre_tag);

    char* buffer = malloc(BLOCK_SIZE + 1);
    if (!buffer) {
        fclose(f_logico);
        log_error_seguro(storage_logger, "##%d - No hay memoria para buffer de lectura en READ", query_id);
        resultado->mensaje = strdup("NO HAY MEMORIA PARA BUFFER DE LECTURA");
        return resultado;
    }

    aplicar_retardo_block();
    size_t leidos = fread(buffer, 1, BLOCK_SIZE, f_logico);
    fclose(f_logico);

    if (leidos != BLOCK_SIZE) {
        free(buffer);
        log_error_seguro(storage_logger, "##%d - Lectura incompleta en READ: %zu de %d bytes", query_id, leidos, BLOCK_SIZE);
        resultado->mensaje = strdup("ERROR LECTURA INCOMPLETA EN READ");
        return resultado;
    }

    buffer[BLOCK_SIZE] = '\0'; // si se trata como string

    log_info_seguro(storage_logger, "##<%d> - Bloque Lógico Leído <%s>:<%s> - Número de Bloque: <%d>",
                    query_id, nombre_file, nombre_tag, bloque_logico);

    resultado->resultado = 1;

    // *** Transferir ownership sin copiar ***
    resultado->mensaje = buffer;   // ahora el caller debe hacer free(resultado->mensaje)
    // Si preferís copiar y liberar buffer:
    // resultado->mensaje = malloc(BLOCK_SIZE + 1);
    // if (!resultado->mensaje) { free(buffer); /* set error, return */ }
    // memcpy(resultado->mensaje, buffer, BLOCK_SIZE + 1);
    // free(buffer);

    return resultado;
}



// bool commit_file_tag(const char* nombre_file, const char* nombre_tag, int query_id) {
//     aplicar_retardo_op();
//     char path_tag[PATH_MAX], path_metadata[PATH_MAX], path_logical_blocks[PATH_MAX], path_hash_index[PATH_MAX];
//     if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
//         snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
//         snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX ||
//         snprintf(path_hash_index, PATH_MAX, "%s/blocks_hash_index.config", PUNTO_MONTAJE) >= PATH_MAX) {
//         log_error_seguro(storage_logger, "##%d - Path truncado en COMMIT", query_id);
//         return false;
//     }

//     pthread_mutex_lock(&mutex_fs);

//     t_config* metadata = config_create(path_metadata);
//     if (!metadata) {
//         log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en COMMIT", query_id);
//         pthread_mutex_unlock(&mutex_fs);
//         return false;
//     }

//     char* estado = config_get_string_value(metadata, "ESTADO");
//     if (!estado || strcmp(estado, "COMMITED") == 0) {
//         log_error_seguro(storage_logger, "##%d - File:Tag ya está COMMITED", query_id);
//         config_destroy(metadata);
//         pthread_mutex_unlock(&mutex_fs);
//         return false;
//     }

//     char** bloques_config = config_get_array_value(metadata, "BLOCKS");
//     int cantidad_bloques = string_array_size(bloques_config);

//     t_config* hash_index = config_create(path_hash_index);
//     if (!hash_index) {
//         FILE* f_dummy = fopen(path_hash_index, "w");
//         if (f_dummy) fclose(f_dummy);
//         hash_index = config_create(path_hash_index);
//     }

//     for (int i = 0; i < cantidad_bloques; i++) {
//         int bloque_fisico = atoi(bloques_config[i]);
//         char path_fisico[PATH_MAX];
//         if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico) >= PATH_MAX) {
//             log_error_seguro(storage_logger, "##%d - Path truncado en bloque físico", query_id);
//             continue;
//         }
//         aplicar_retardo_block();
//         FILE* f = fopen(path_fisico, "rb");
//         if (!f) continue;

//         unsigned char buffer[BLOCK_SIZE];
//         if (fread(buffer, 1, BLOCK_SIZE, f) != BLOCK_SIZE) {
//             fclose(f);
//             continue;
//         }
//         fclose(f);

//         char* hash_str = crypto_md5(buffer, BLOCK_SIZE);
//         if (!hash_str) continue;

//         char* bloque_existente = config_get_string_value(hash_index, hash_str);
//         if (!bloque_existente) {
//             config_set_value(hash_index, hash_str, bloques_config[i]);
//         } else if (strcmp(bloques_config[i], bloque_existente) != 0) {
//             // Deduplicación activa
//             int bloque_fisico_existente = atoi(bloque_existente);

//             char path_fisico_existente[PATH_MAX];
//             if (snprintf(path_fisico_existente, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico_existente) >= PATH_MAX) {
//                 log_error_seguro(storage_logger, "##%d - Path truncado en bloque físico existente", query_id);
//                 free(hash_str);
//                 continue;
//             }

//             char path_logico[PATH_MAX];
//             if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
//                 log_error_seguro(storage_logger, "##%d - Path truncado en bloque lógico", query_id);
//                 free(hash_str);
//                 continue;
//             }
//             aplicar_retardo_block();
//             unlink(path_logico);
//             link(path_fisico_existente, path_logico);

//             bloques_config[i] = string_from_format("%d", bloque_fisico_existente);

//             if (contar_referencias_bloque(bloque_fisico) == 0) {
//                 aplicar_retardo_block();
//                 liberar_bloque_fisico(bloque_fisico);
//                 log_info_seguro(storage_logger, "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, bloque_fisico);
//             }

//             log_info_seguro(storage_logger, "##%d - Deduplicación: bloque lógico %d ahora apunta a bloque físico %d (hash %s)",
//                             query_id, i, bloque_fisico_existente, hash_str);
//         }

//         free(hash_str);
//     }

//     int tamanio = config_get_int_value(metadata, "TAMAÑO");
//     FILE* f_meta = fopen(path_metadata, "w");
//     if (!f_meta) {
//         log_error_seguro(storage_logger, "##%d - No se pudo abrir metadata.config para escritura en COMMIT", query_id);
//         string_array_destroy(bloques_config);
//         config_destroy(metadata);
//         config_destroy(hash_index);
//         pthread_mutex_unlock(&mutex_fs);
//         return false;
//     }

//     fprintf(f_meta, "TAMAÑO=%d\nBLOCKS=[", tamanio);
//     for (int i = 0; bloques_config[i] != NULL; i++) {
//         fprintf(f_meta, "%s%s", i == 0 ? "" : ",", bloques_config[i]);
//     }
//     fprintf(f_meta, "]\nESTADO=COMMITED\n");
//     fclose(f_meta);

//     config_save_in_file(hash_index, path_hash_index);

//     log_info_seguro(storage_logger, "##%d - COMMIT exitoso: %s:%s", query_id, nombre_file, nombre_tag);

//     string_array_destroy(bloques_config);
//     config_destroy(metadata);
//     config_destroy(hash_index);
//     pthread_mutex_unlock(&mutex_fs);
//     return true;
// }



char* commit_file_tag(const char* nombre_file, const char* nombre_tag, int query_id) {
    aplicar_retardo_op();

    t_config* metadata = NULL;
    t_config* hash_index = NULL;
    char** bloques_config = NULL;

    char path_tag[PATH_MAX], path_metadata[PATH_MAX], path_logical_blocks[PATH_MAX], path_hash_index[PATH_MAX];

    if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX ||
        snprintf(path_hash_index, PATH_MAX, "%s/blocks_hash_index.config", PUNTO_MONTAJE) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en COMMIT", query_id);
        return "ERROR PATH TRUNCADO EN COMMIT";
    }

    /* Orden: proteger hash_index y luego el tag (no tomar bitmap externo) */
    pthread_mutex_lock(&mutex_hash_index);
    lock_tag(nombre_file, nombre_tag);

    metadata = config_create(path_metadata);
    if (!metadata) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en COMMIT", query_id);
        return "NO SE PUDO LEER METADATA.CONFIG EN COMMIT";
    }

    {
        char* estado = config_get_string_value(metadata, "ESTADO");
        if (!estado || strcmp(estado, "COMMITED") == 0) {
            log_error_seguro(storage_logger, "##%d - File:Tag ya está COMMITED", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "FILE:TAG YA ESTA COMMITED";
        }
    }

    bloques_config = config_get_array_value(metadata, "BLOCKS");
    if (!bloques_config) {
        log_error_seguro(storage_logger, "##%d - Metadata sin BLOCKS en COMMIT", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "METADATA SIN BLOCKS EN COMMIT";
    }

    int cantidad_bloques = string_array_size(bloques_config);

    /* Abrir/crear índice global de hashes */
    hash_index = config_create(path_hash_index);
    if (!hash_index) {
        FILE* f_dummy = fopen(path_hash_index, "w");
        if (f_dummy) fclose(f_dummy);
        hash_index = config_create(path_hash_index);
    }
    if (!hash_index) {
        log_error_seguro(storage_logger, "##%d - No se pudo abrir/crear blocks_hash_index.config", query_id);
        unlock_tag(nombre_file, nombre_tag);
        return "NO SE PUDO ABRIR/CREAR BLOCK_HASH_INDEX.CONFIG";
    }

    /* Recorrer bloques lógicos del tag */
    for (int i = 0; i < cantidad_bloques; i++) {
        int bloque_fisico = atoi(bloques_config[i]);

        char path_fisico[PATH_MAX];
        if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico) >= PATH_MAX) {
            log_error_seguro(storage_logger, "##%d - Path truncado en bloque físico", query_id);
            unlock_tag(nombre_file, nombre_tag);
            continue;
        }

        aplicar_retardo_block();
        FILE* f = fopen(path_fisico, "rb");
        if (!f) continue;

        unsigned char buffer[BLOCK_SIZE];
        size_t rd = fread(buffer, 1, BLOCK_SIZE, f);
        fclose(f);
        if (rd != BLOCK_SIZE) {
            continue;
        }

        char* hash_str = crypto_md5(buffer, BLOCK_SIZE); /* malloc'd */
        if (!hash_str) continue;

        char* bloque_existente = config_get_string_value(hash_index, hash_str);
        if (!bloque_existente) {
            /* Nuevo contenido: registrar hash -> bloque_fisico actual */
            config_set_value(hash_index, hash_str, bloques_config[i]);
        } else if (strcmp(bloques_config[i], bloque_existente) != 0) {
            /* Deduplicación activa: reapuntar el bloque lógico i al bloque físico existente */
            int bloque_fisico_existente = atoi(bloque_existente);

            char path_fisico_existente[PATH_MAX];
            if (snprintf(path_fisico_existente, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico_existente) >= PATH_MAX) {
                log_error_seguro(storage_logger, "##%d - Path truncado en bloque físico existente", query_id);
                free(hash_str);
                continue;
            }

            char path_logico[PATH_MAX];
            if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
                log_error_seguro(storage_logger, "##%d - Path truncado en bloque lógico", query_id);
                free(hash_str);
                continue;
            }

            aplicar_retardo_block();
            unlink(path_logico);
            link(path_fisico_existente, path_logico);

            /* Actualizar metadata: liberar string viejo antes de asignar el nuevo */
            char* nuevo = string_from_format("%d", bloque_fisico_existente);
            if (nuevo) {
                free(bloques_config[i]);
                bloques_config[i] = nuevo;
            }

            /* Intentar liberar el físico anterior si ya no tiene referencias (libera internamente con bitmap) */
            if (contar_referencias_bloque(bloque_fisico) == 0) {
                aplicar_retardo_block();
                liberar_bloque_fisico(bloque_fisico);
                log_info_seguro(storage_logger, "##<%d> - Bloque Físico Liberado - Número de Bloque: <%d>", query_id, bloque_fisico);
            }

            log_info_seguro(storage_logger,
                            "##<%d> - <%s>:<%s> Bloque Logico <%d> se reasigna de <%d> a <%d>",
                            query_id, nombre_file,nombre_tag, i, bloque_fisico, bloque_fisico_existente);
        }

        free(hash_str);
    }

    /* Reescribir metadata a estado COMMITED */
    {
        int tamanio = config_get_int_value(metadata, "TAMAÑO");
        FILE* f_meta = fopen(path_metadata, "w");
        if (!f_meta) {
            log_error_seguro(storage_logger, "##%d - No se pudo abrir metadata.config para escritura en COMMIT", query_id);
            unlock_tag(nombre_file, nombre_tag);
            return "ERROR APERTURA METADATA.CONFIG EN COMMIT";
        }

        fprintf(f_meta, "TAMAÑO=%d\nBLOCKS=[", tamanio);
        for (int i = 0; bloques_config[i] != NULL; i++) {
            fprintf(f_meta, "%s%s", i == 0 ? "" : ",", bloques_config[i]);
        }
        fprintf(f_meta, "]\nESTADO=COMMITED\n");
        fclose(f_meta);
    }

    /* Guardar índice global de hashes */
    config_save_in_file(hash_index, path_hash_index);

    log_info_seguro(storage_logger, "##%d - COMMIT exitoso: %s:%s", query_id, nombre_file, nombre_tag);

    if (bloques_config) string_array_destroy(bloques_config);
    if (metadata) config_destroy(metadata);
    if (hash_index) config_destroy(hash_index);
    unlock_tag(nombre_file, nombre_tag);
    pthread_mutex_unlock(&mutex_hash_index);
    return "OPERACION EXITOSA";
}


char* copy_tag_file(const char* file_origen, const char* tag_origen,
                   const char* file_destino, const char* tag_destino, int query_id){
    aplicar_retardo_op();

    char path_file_origen[PATH_MAX], path_tag_origen[PATH_MAX];
    char path_file_destino[PATH_MAX], path_tag_destino[PATH_MAX];
    char path_metadata_origen[PATH_MAX], path_metadata_destino[PATH_MAX];
    char path_logical_blocks_origen[PATH_MAX], path_logical_blocks_destino[PATH_MAX];

    if (snprintf(path_file_origen, PATH_MAX, "%s/%s", path_files, file_origen) >= PATH_MAX ||
        snprintf(path_tag_origen, PATH_MAX, "%s/%s", path_file_origen, tag_origen) >= PATH_MAX ||
        snprintf(path_metadata_origen, PATH_MAX, "%s/metadata.config", path_tag_origen) >= PATH_MAX ||
        snprintf(path_logical_blocks_origen, PATH_MAX, "%s/logical_blocks", path_tag_origen) >= PATH_MAX ||
        snprintf(path_file_destino, PATH_MAX, "%s/%s", path_files, file_destino) >= PATH_MAX ||
        snprintf(path_tag_destino, PATH_MAX, "%s/%s", path_file_destino, tag_destino) >= PATH_MAX ||
        snprintf(path_metadata_destino, PATH_MAX, "%s/metadata.config", path_tag_destino) >= PATH_MAX ||
        snprintf(path_logical_blocks_destino, PATH_MAX, "%s/logical_blocks", path_tag_destino) >= PATH_MAX)
    {
        log_error_seguro(storage_logger, "##%d - Path truncado en TAG (%s:%s -> %s:%s)",
                         query_id, file_origen, tag_origen, file_destino, tag_destino);
        return "PATH_TRUNCADO_AL_HACER_COPY_TAG";
    }

    /* Orden de bloqueo por clave para evitar deadlocks:
       bloqueamos primero la clave lexicográficamente menor, luego la mayor. */
    char* key_origen  = string_from_format("%s/%s", file_origen,  tag_origen);
    char* key_destino = string_from_format("%s/%s", file_destino, tag_destino);
    bool lock_origen_primero = strcmp(key_origen, key_destino) <= 0;

    if (lock_origen_primero) {
        lock_tag(file_origen,  tag_origen);
        lock_tag(file_destino, tag_destino);
    } else {
        lock_tag(file_destino, tag_destino);
        lock_tag(file_origen,  tag_origen);
    }
    free(key_origen);
    free(key_destino);

    /* Validaciones bajo locks para evitar condiciones de carrera:
       - origen debe existir (tag y metadata)
       - destino no debe existir aún
    */
    if (access(path_tag_origen, F_OK) == -1 || access(path_metadata_origen, F_OK) == -1) {
        log_error_seguro(storage_logger, "##%d - TAG falló: Tag de origen inexistente (%s:%s)",
                         query_id, file_origen, tag_origen);
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "TAG_INEXISTENTE";
    }
    if (access(path_tag_destino, F_OK) != -1) {
        log_error_seguro(storage_logger, "##%d - TAG falló: Tag destino ya existe (%s:%s)",
                         query_id, file_destino, tag_destino);
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "TAG_DESTINO_EXISTENTE";
    }

    /* Crear estructura de destino */
    if (mkdir(path_file_destino, 0777) == -1 && errno != EEXIST) {
        perror("Error al crear directorio del file de destino");
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_AL_CREAR_DIRECTORIO_DEL_FILE_DESTINO";
    }
    if (mkdir(path_tag_destino, 0777) == -1) {
        perror("Error al crear directorio del tag de destino");
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_AL_CREAR_DIRECTORIO_DEL_TAG_DESTINO";
    }
    if (mkdir(path_logical_blocks_destino, 0777) == -1) {
        perror("Error al crear directorio logical_blocks de destino");
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_AL_CREAR_DIRECTORIO_LOGICAL_BLOCKS_DESTINO";
    }

    /* Leer metadata de origen */
    t_config* meta_origen = config_create(path_metadata_origen);
    if (!meta_origen) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config de origen en TAG", query_id);
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_AL_LEER_METADATA_DE_TAG_ORIGEN";
    }

    int tamanio_origen   = config_get_int_value(meta_origen, "TAMAÑO");
    char** blocks_origen = config_get_array_value(meta_origen, "BLOCKS");
    if (!blocks_origen) {
        log_error_seguro(storage_logger, "##%d - Metadata de origen sin BLOCKS en TAG", query_id);
        config_destroy(meta_origen);
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_METADATA_DE_ORIGEN_SIN_BLOCKS_EN_TAG";
    }

    int cant_bloques = string_array_size(blocks_origen);

    /* Crear metadata de destino (WORK_IN_PROGRESS con mismos bloques) */
    FILE* f_meta_destino = fopen(path_metadata_destino, "w");
    if (!f_meta_destino) {
        log_error_seguro(storage_logger, "##%d - No se pudo crear metadata.config de destino en TAG", query_id);
        string_array_destroy(blocks_origen);
        config_destroy(meta_origen);
        unlock_tag(file_destino, tag_destino);
        unlock_tag(file_origen,  tag_origen);
        return "FALLO_AL_CREAR_METADATA_DE_DESTINO_EN_TAG";
    }

    fprintf(f_meta_destino, "TAMAÑO=%d\nBLOCKS=[", tamanio_origen);
    for (int i = 0; i < cant_bloques; i++) {
        fprintf(f_meta_destino, "%s%s", i == 0 ? "" : ",", blocks_origen[i]);
    }
    fprintf(f_meta_destino, "]\nESTADO=WORK_IN_PROGRESS\n");
    fclose(f_meta_destino);

    /* Replicar hard links destino -> mismos bloques físicos del origen */
    for (int i = 0; i < cant_bloques; i++) {
        int bloque_fisico = atoi(blocks_origen[i]);

        char path_fisico[PATH_MAX];
        char path_logico_destino_i[PATH_MAX];

        if (snprintf(path_fisico, PATH_MAX, "%s/block%04d.dat", path_blocks, bloque_fisico) >= PATH_MAX ||
            snprintf(path_logico_destino_i, PATH_MAX, "%s/%06d.dat", path_logical_blocks_destino, i) >= PATH_MAX)
        {
            log_error_seguro(storage_logger, "##%d - Path truncado en TAG (fisico/logico destino)", query_id);
            continue;
        }

        aplicar_retardo_block();
        if (link(path_fisico, path_logico_destino_i) == -1) {
            perror("Error al intentar crear hard link en TAG");
            continue;
        }

        log_info_seguro(storage_logger,
                        "##<%d> - <%s>:<%s> Se agregó el hard link del bloque lógico <%d> al bloque físico <%d>",
                        query_id, file_destino, tag_destino, i, bloque_fisico);
    }

    log_info_seguro(storage_logger, "##<%d> - Tag creado <%s>:<%s>", query_id, file_destino, tag_destino);

    /* Limpieza y desbloqueo */
    string_array_destroy(blocks_origen);
    config_destroy(meta_origen);

    unlock_tag(file_destino, tag_destino);
    unlock_tag(file_origen,  tag_origen);

    return "OPERACION EXITOSA";
}



// char* eliminar_tag(const char* nombre_file, const char* nombre_tag, int query_id) {
//     aplicar_retardo_op();

//     //bool rc=false
//     t_config* metadata = NULL;
//     char** bloques_config = NULL;

//     char path_tag[PATH_MAX], path_metadata[PATH_MAX], path_logical_blocks[PATH_MAX];

//     if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
//         snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
//         snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
//         log_error_seguro(storage_logger, "##%d - Path truncado en DELETE", query_id);
//         return "PATH_TRUNCADO_EN_DELETE";
//     }

//     /* Sólo lock por File:Tag. No envolver con mutex_bitmap externo. */
//     lock_tag(nombre_file, nombre_tag);

//     if (access(path_metadata, F_OK) == -1) {
//         log_error_seguro(storage_logger, "##%d - DELETE falló: File:Tag inexistente (%s:%s)", query_id, nombre_file, nombre_tag);
//         if (bloques_config) string_array_destroy(bloques_config);
//         if (metadata) config_destroy(metadata);
//         unlock_tag(nombre_file, nombre_tag);

//         /* Remover el mutex del File:Tag del diccionario (ya liberado) */

//         remove_tag_lock(nombre_file, nombre_tag);


//         return "FILE/TAG_INEXISTENTE";

//     }

//     metadata = config_create(path_metadata);
//     if (!metadata) {
//         log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en DELETE", query_id);
//         if (bloques_config) string_array_destroy(bloques_config);
//         if (metadata) config_destroy(metadata);
//         unlock_tag(nombre_file, nombre_tag);

//         /* Remover el mutex del File:Tag del diccionario (ya liberado) */

//         remove_tag_lock(nombre_file, nombre_tag);


//         return "NO_SE_PUDO_LEER_METADATA";
//     }

//     bloques_config = config_get_array_value(metadata, "BLOCKS");
//     if (!bloques_config) {
//         log_error_seguro(storage_logger, "##%d - Metadata sin BLOCKS en DELETE", query_id);
//         if (bloques_config) string_array_destroy(bloques_config);
//         if (metadata) config_destroy(metadata);
//         unlock_tag(nombre_file, nombre_tag);

//         /* Remover el mutex del File:Tag del diccionario (ya liberado) */

//         remove_tag_lock(nombre_file, nombre_tag);


//         return "FILE/TAG_INEXISTENTE";
//     }

//     int cant_bloques = string_array_size(bloques_config);

//     /* Eliminar hard links de cada bloque lógico y liberar físicos cuando corresponda.
//        liberar_bloque_fisico bloquea bitmap internamente. */
//     for (int i = 0; i < cant_bloques; i++) {
//         int bloque_fisico = atoi(bloques_config[i]);

//         char path_logico[PATH_MAX];
//         if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
//             log_error_seguro(storage_logger, "##%d - Path truncado en DELETE (logico)", query_id);
//             continue;
//         }

//         aplicar_retardo_block();
//         unlink(path_logico);

//         log_info_seguro(storage_logger,
//                         "##<%d> - <%s>:<%s> Se eliminó el hard link del bloque lógico <%d> al bloque físico <%d>",
//                         query_id, nombre_file, nombre_tag, i, bloque_fisico);

//         if (contar_referencias_bloque(bloque_fisico) == 0) {
//             aplicar_retardo_block();
//             liberar_bloque_fisico(bloque_fisico);
//             log_info_seguro(storage_logger,
//                             "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, bloque_fisico);
//         }
//     }

//     /* Borrar metadata y directorios del tag */
//     unlink(path_metadata);
//     rmdir(path_logical_blocks);
//     rmdir(path_tag);

//     log_info_seguro(storage_logger, "##%d - Tag Eliminado %s:%s", query_id, nombre_file, nombre_tag);
//     unlock_tag(nombre_file,nombre_tag);
//     return "OPERACION EXITOSA";

// // cleanup:
// //     if (bloques_config) string_array_destroy(bloques_config);
// //     if (metadata) config_destroy(metadata);
// //     unlock_tag(nombre_file, nombre_tag);

// //     /* Remover el mutex del File:Tag del diccionario (ya liberado) */
// //     if (rc) {
// //         remove_tag_lock(nombre_file, nombre_tag);
// //     }

// //     return rc;
// }


char* eliminar_tag(const char* nombre_file, const char* nombre_tag, int query_id) {
    aplicar_retardo_op();
    if(strcmp(nombre_file,"initial_file")==0 && strcmp(nombre_tag,"BASE")==0){
        return "NO SE PUEDE ELIMINAR initial_file:BASE";
    }
    t_config* metadata = NULL;
    char** bloques_config = NULL;

    char path_tag[PATH_MAX], path_metadata[PATH_MAX], path_logical_blocks[PATH_MAX];

    if (snprintf(path_tag, PATH_MAX, "%s/%s/%s", path_files, nombre_file, nombre_tag) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag) >= PATH_MAX) {
        log_error_seguro(storage_logger, "##%d - Path truncado en DELETE", query_id);
        return "PATH_TRUNCADO_EN_DELETE";
    }

    lock_tag(nombre_file, nombre_tag);

    // --- Validar existencia ---
    if (access(path_metadata, F_OK) == -1) {
        log_error_seguro(storage_logger, "##%d - DELETE falló: File:Tag inexistente (%s:%s)",
                         query_id, nombre_file, nombre_tag);
        unlock_tag(nombre_file, nombre_tag);
        remove_tag_lock(nombre_file, nombre_tag);
        return "FILE/TAG_INEXISTENTE";
    }

    metadata = config_create(path_metadata);
    if (!metadata) {
        log_error_seguro(storage_logger, "##%d - No se pudo leer metadata.config en DELETE", query_id);
        unlock_tag(nombre_file, nombre_tag);
        remove_tag_lock(nombre_file, nombre_tag);
        return "NO_SE_PUDO_LEER_METADATA";
    }

    bloques_config = config_get_array_value(metadata, "BLOCKS");
    if (!bloques_config) {
        log_error_seguro(storage_logger, "##%d - Metadata sin BLOCKS en DELETE", query_id);
        // liberar metadata antes de salir
        config_destroy(metadata);
        unlock_tag(nombre_file, nombre_tag);
        remove_tag_lock(nombre_file, nombre_tag);
        return "METADATA_SIN_BLOCKS_EN_DELETE";
    }

    // --- Eliminar links lógicos y liberar bloques físicos cuando corresponda ---
    int cant_bloques = string_array_size(bloques_config);
    for (int i = 0; i < cant_bloques; i++) {
        int bloque_fisico = atoi(bloques_config[i]);

        char path_logico[PATH_MAX];
        if (snprintf(path_logico, PATH_MAX, "%s/%06d.dat", path_logical_blocks, i) >= PATH_MAX) {
            log_error_seguro(storage_logger, "##%d - Path truncado en DELETE (logico)", query_id);
            continue; // seguir con los restantes
        }

        aplicar_retardo_block();
        unlink(path_logico);

        log_info_seguro(storage_logger,
                        "##<%d> - <%s>:<%s> Se eliminó el hard link del bloque lógico <%d> al bloque físico <%d>",
                        query_id, nombre_file, nombre_tag, i, bloque_fisico);

        if (contar_referencias_bloque(bloque_fisico) == 0) {
            aplicar_retardo_block();
            liberar_bloque_fisico(bloque_fisico);
            log_info_seguro(storage_logger,
                            "##%d - Bloque Físico Liberado - Número de Bloque: %d",
                            query_id, bloque_fisico);
        }
    }

    // --- Borrar metadata y directorios del tag ---
    unlink(path_metadata);
    rmdir(path_logical_blocks);
    rmdir(path_tag);

    log_info_seguro(storage_logger, "##%d - Tag Eliminado %s:%s",
                    query_id, nombre_file, nombre_tag);

    // --- Cleanup común ---
    string_array_destroy(bloques_config);
    config_destroy(metadata);
    unlock_tag(nombre_file, nombre_tag);
    remove_tag_lock(nombre_file, nombre_tag);

    return "OPERACION EXITOSA";
}
