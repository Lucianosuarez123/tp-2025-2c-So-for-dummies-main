#include "../includes/iniciarStorage.h"


void inicializar_storage(){

    iniciar_config();
    iniciar_loggers();

    lista_workers=list_create();
    tag_locks=dictionary_create();
    pthread_mutex_init(&mutex_lista_workers, NULL);
    pthread_mutex_init(&mutex_fs, NULL);
    pthread_mutex_init(&mutex_bitmap, NULL);
    pthread_mutex_init(&mutex_blocks, NULL);
    pthread_mutex_init(&mutex_hash_index, NULL);
    pthread_mutex_init(&g_tag_map_mutex, NULL);
    if (string_equals_ignore_case(FRESH_START, "TRUE")){
        formatear_fs();
    }
    else {
        cargar_estructuras_preexistentes();
    }
    verificar_initial_file_base();
}

void iniciar_config(){
    char exe_path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (len == -1) {
        perror("No se pudo obtener la ruta del ejecutable");
        exit(EXIT_FAILURE);
    }
    exe_path[len] = '\0';

    // Obtener el directorio del ejecutable
    char* dir = dirname(exe_path);

    // Intentar con ./storage.config
    char config_path[PATH_MAX];
    snprintf(config_path, sizeof(config_path), "%s/../../utils/tests/%s.config", dir, archivo_config);

    storage_config = config_create(config_path);

    // Si no está, intentar con ../storage.config
    if (storage_config == NULL) {
        snprintf(config_path, sizeof(config_path), "%s/../../utils/tests/%s.config", dir, archivo_config);

        storage_config = config_create(config_path);
    }

    if (storage_config == NULL) {
        perror("No se pudo cargar el archivo de configuración");
        exit(EXIT_FAILURE);
    }

    //para cargar superblock.config

    snprintf(config_path, sizeof(config_path), "%s/%s.config", dir, "superblock");

    superblock_config = config_create(config_path);

    // Si no está, intentar con ../superblock.config
    if (superblock_config == NULL) {
        snprintf(config_path, sizeof(config_path), "%s/../%s.config", dir, "superblock");

        superblock_config = config_create(config_path);
    }

    if (superblock_config == NULL) {
        perror("No se pudo cargar el archivo de configuración para superblock.config");
        exit(EXIT_FAILURE);
    }

    PUERTO_ESCUCHA = config_get_string_value(storage_config, "PUERTO_ESCUCHA");
    FRESH_START = config_get_string_value(storage_config, "FRESH_START");
    PUNTO_MONTAJE = config_get_string_value(storage_config, "PUNTO_MONTAJE");
    RETARDO_OPERACION = config_get_int_value(storage_config, "RETARDO_OPERACION");
    RETARDO_ACCESO_BLOQUE = config_get_int_value(storage_config, "RETARDO_ACCESO_BLOQUE");
    LOG_LEVEL = config_get_string_value(storage_config, "LOG_LEVEL");
    FS_SIZE= config_get_int_value(superblock_config, "FS_SIZE");
    BLOCK_SIZE=config_get_int_value(superblock_config, "BLOCK_SIZE");

    if(PUERTO_ESCUCHA == NULL || FRESH_START == NULL || PUNTO_MONTAJE == NULL || RETARDO_OPERACION < 0 || RETARDO_ACCESO_BLOQUE< 0 || LOG_LEVEL==NULL|| FS_SIZE < 0 || BLOCK_SIZE<0){
    printf("Error al intentar cargar el config.");
    exit(EXIT_FAILURE);}


    snprintf(path_bitmap, sizeof(path_bitmap), "%s/bitmap.bin", PUNTO_MONTAJE);
    snprintf(path_blocks, sizeof(path_blocks), "%s/physical_blocks", PUNTO_MONTAJE);
    snprintf(path_files, sizeof(path_files), "%s/files", PUNTO_MONTAJE);
    snprintf(path_hash_index, sizeof(path_hash_index), "%s/blocks_hash_index.config", PUNTO_MONTAJE);
    snprintf(path_superblock, sizeof(path_superblock), "%s/superblock.config", PUNTO_MONTAJE);


}
void iniciar_loggers() {
    char* path_log = string_from_format("./%s.log", "STORAGE");
    char* path_log_formateado = string_from_format(path_log, "STORAGE");
    remove(path_log_formateado);
    storage_logger = log_create(path_log_formateado, "STORAGE", 1, log_level_from_string(LOG_LEVEL));
    free(path_log);
    free(path_log_formateado);

    if (storage_logger == NULL) {
        perror("No se pudo crear o encontrar el archivo. (log storage_logger)");
        exit(EXIT_FAILURE);
    }

}

void cargar_estructuras_preexistentes() {
    log_debug_seguro(storage_logger, "Cargando estructuras existentes del File System...");

    int cantidad_bloques = FS_SIZE / BLOCK_SIZE;
    size_t bitmap_size = cantidad_bloques / 8;

    // Verificar existencia de estructuras mínimas
    if (access(path_superblock, F_OK) == -1) {
        log_error_seguro(storage_logger, "Falta superblock.config. ¿FRESH_START mal configurado?");
        exit(EXIT_FAILURE);
    }

    if (access(path_blocks, F_OK) == -1 || access(path_files, F_OK) == -1 || access(path_hash_index, F_OK) == -1) {
        log_error_seguro(storage_logger, "Estructuras del FS incompletas. ¿FRESH_START mal configurado?");
        exit(EXIT_FAILURE);
    }

    // Mapear bitmap.bin
    fd_bitmap = open(path_bitmap, O_RDWR);
    if (fd_bitmap == -1) {
        perror("Error abriendo bitmap.bin");
        exit(EXIT_FAILURE);
    }

    struct stat stat_bitmap;
    if (fstat(fd_bitmap, &stat_bitmap) == -1) {
        perror("Error obteniendo tamaño de bitmap.bin");
        close(fd_bitmap);
        exit(EXIT_FAILURE);
    }

    if ((size_t)stat_bitmap.st_size != bitmap_size) {
        log_error(storage_logger, "Tamaño de bitmap.bin incorrecto. ¿FS corrupto?");
        close(fd_bitmap);
        exit(EXIT_FAILURE);
    }

    bitmap_mem = mmap(NULL, bitmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_bitmap, 0);
    if (bitmap_mem == MAP_FAILED) {
        perror("Error en mmap de bitmap.bin");
        close(fd_bitmap);
        exit(EXIT_FAILURE);
    }

    bitmap = bitarray_create_with_mode(bitmap_mem, bitmap_size, LSB_FIRST);

    // Verificar existencia de bloques físicos
    for (int i = 0; i < cantidad_bloques; i++) {
    if (bitarray_test_bit(bitmap, i)) {
        char path_bloque[PATH_MAX];
        if (snprintf(path_bloque, PATH_MAX, "%s/block%04d.dat", path_blocks, i) >= PATH_MAX) {
            log_error_seguro(storage_logger, "Path de bloque físico demasiado largo");
            exit(EXIT_FAILURE);
        }

        struct stat stat_bloque;
        if (access(path_bloque, F_OK) == -1 || stat(path_bloque, &stat_bloque) == -1) {
            log_error_seguro(storage_logger, "Falta bloque físico %d marcado como ocupado", i);
            exit(EXIT_FAILURE);
        }

        if (stat_bloque.st_size != BLOCK_SIZE) {
            log_error_seguro(storage_logger, "Bloque físico %d tiene tamaño incorrecto", i);
            exit(EXIT_FAILURE);
        }
    }
}

    log_debug_seguro(storage_logger, "Estructuras del File System cargadas correctamente.");
}

void formatear_fs() {
    int cantidad_bloques = FS_SIZE / BLOCK_SIZE;
    size_t bitmap_size = cantidad_bloques / 8;

    log_debug_seguro(storage_logger, "Formateando File System desde cero...");

    // Asignar paths a variables globales con chequeo de truncamiento
    if (snprintf(path_blocks, PATH_MAX, "%s/physical_blocks", PUNTO_MONTAJE) >= PATH_MAX ||
        snprintf(path_files, PATH_MAX, "%s/files", PUNTO_MONTAJE) >= PATH_MAX ||
        snprintf(path_superblock, PATH_MAX, "%s/superblock.config", PUNTO_MONTAJE) >= PATH_MAX ||
        snprintf(path_bitmap, PATH_MAX, "%s/bitmap.bin", PUNTO_MONTAJE) >= PATH_MAX ||
        snprintf(path_hash_index, PATH_MAX, "%s/blocks_hash_index.config", PUNTO_MONTAJE) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path truncado al asignar variables globales");
        exit(EXIT_FAILURE);
    }
    char comando[PATH_MAX + 50];
    snprintf(comando, sizeof(comando), "rm -rf %s/*", PUNTO_MONTAJE);
    system(comando);
    // Crear directorios base
    if (mkdir(PUNTO_MONTAJE, 0777) == -1 && errno != EEXIST) {
        perror("Error creando punto de montaje");
        exit(EXIT_FAILURE);
    }

    if (mkdir(path_blocks, 0777) == -1 && errno != EEXIST) {
        perror("Error creando directorio physical_blocks");
        exit(EXIT_FAILURE);
    }

    if (mkdir(path_files, 0777) == -1 && errno != EEXIST) {
        perror("Error creando directorio files");
        exit(EXIT_FAILURE);
    }

    // Crear superblock.config
    FILE* superblock = fopen(path_superblock, "w");
    if (!superblock) {
        perror("Error creando superblock.config");
        exit(EXIT_FAILURE);
    }
    fprintf(superblock, "FS_SIZE=%d\nBLOCK_SIZE=%d\n", FS_SIZE, BLOCK_SIZE);
    fclose(superblock);

    // Crear y mapear bitmap.bin
    fd_bitmap = open(path_bitmap, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd_bitmap == -1) {
        perror("Error creando bitmap.bin");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(fd_bitmap, bitmap_size) == -1) {
        perror("Error al truncar bitmap.bin");
        close(fd_bitmap);
        exit(EXIT_FAILURE);
    }

    bitmap_mem = mmap(NULL, bitmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_bitmap, 0);
    if (bitmap_mem == MAP_FAILED) {
        perror("Error en mmap de bitmap.bin");
        close(fd_bitmap);
        exit(EXIT_FAILURE);
    }

    bitmap = bitarray_create_with_mode(bitmap_mem, bitmap_size, LSB_FIRST);
    for (int i = 0; i < cantidad_bloques; i++) {
        bitarray_clean_bit(bitmap, i);
    }

    msync(bitmap_mem, bitmap_size, MS_SYNC);

    // Crear bloque físico 0
    char path_bloque_fisico[PATH_MAX];
    if (snprintf(path_bloque_fisico, PATH_MAX, "%s/block0000.dat", path_blocks) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path demasiado largo para bloque físico 0");
        exit(EXIT_FAILURE);
    }

    int fd_bloque = open(path_bloque_fisico, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd_bloque == -1) {
        perror("Error creando bloque físico 0");
        exit(EXIT_FAILURE);
    }

    if (ftruncate(fd_bloque, BLOCK_SIZE) == -1) {
        perror("Error truncando bloque físico 0");
        close(fd_bloque);
        exit(EXIT_FAILURE);
    }

    void* bloque_mem = mmap(NULL, BLOCK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd_bloque, 0);
    if (bloque_mem == MAP_FAILED) {
        perror("Error en mmap de bloque físico 0");
        close(fd_bloque);
        exit(EXIT_FAILURE);
    }

    memset(bloque_mem, '0', BLOCK_SIZE);
    msync(bloque_mem, BLOCK_SIZE, MS_SYNC);
    munmap(bloque_mem, BLOCK_SIZE);
    close(fd_bloque);

    // Crear initial_file con Tag BASE
    char path_initial_file[PATH_MAX];
    char path_tag_base[PATH_MAX];
    char path_metadata[PATH_MAX];
    char path_logical_blocks[PATH_MAX];
    char path_hardlink[PATH_MAX];

    if (snprintf(path_initial_file, PATH_MAX, "%s/initial_file", path_files) >= PATH_MAX ||
        snprintf(path_tag_base, PATH_MAX, "%s/BASE", path_initial_file) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag_base) >= PATH_MAX ||
        snprintf(path_logical_blocks, PATH_MAX, "%s/logical_blocks", path_tag_base) >= PATH_MAX ||
        snprintf(path_hardlink, PATH_MAX, "%s/000000.dat", path_logical_blocks) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path demasiado largo en creación de initial_file:BASE");
        exit(EXIT_FAILURE);
    }

    mkdir(path_initial_file, 0777);
    mkdir(path_tag_base, 0777);
    mkdir(path_logical_blocks, 0777);

    FILE* metadata = fopen(path_metadata, "w");
    if (!metadata) {
        perror("Error creando metadata.config");
        exit(EXIT_FAILURE);
    }
    fprintf(metadata, "TAMAÑO=%d\nBLOCKS=[0]\nESTADO=WORK_IN_PROGRESS\n", BLOCK_SIZE);
    fclose(metadata);

    if (link(path_bloque_fisico, path_hardlink) == -1) {
        perror("Error creando hard link del bloque lógico 0");
        exit(EXIT_FAILURE);
    }
    bitarray_set_bit(bitmap, 0);
    msync(bitmap_mem, bitmap_size, MS_SYNC);


    // === COMMIT EMBEBIDO de initial_file:BASE ===
    // 1) Registrar el hash del bloque físico 0 -> "0" en el índice global

    t_config* hash_index = config_create(path_hash_index);
    if (!hash_index) {
        FILE* f_dummy = fopen(path_hash_index, "w");
        if (f_dummy) fclose(f_dummy);
        hash_index = config_create(path_hash_index);
    }
    if (!hash_index) {
        log_error_seguro(storage_logger, "No se pudo abrir/crear blocks_hash_index.config en formatear_fs");
        exit(EXIT_FAILURE);
    }

    aplicar_retardo_block();
    FILE* f0 = fopen(path_bloque_fisico, "rb");
    if (!f0) {
        perror("No se pudo abrir bloque físico 0 para hash base");
        config_destroy(hash_index);
        exit(EXIT_FAILURE);
    }

    unsigned char b0[BLOCK_SIZE];
    size_t rd0 = fread(b0, 1, BLOCK_SIZE, f0);
    fclose(f0);
    if (rd0 != BLOCK_SIZE) {
        log_error_seguro(storage_logger, "Lectura incompleta de bloque 0 al registrar hash base");
        config_destroy(hash_index);
        exit(EXIT_FAILURE);
    }

    char* h0 = crypto_md5(b0, BLOCK_SIZE);   // malloc'd
    if (!h0) {
        log_error_seguro(storage_logger, "crypto_md5 falló al registrar hash base");
        config_destroy(hash_index);
        exit(EXIT_FAILURE);
    }

    // Fuerza: hash del “vacío” -> "0"
    config_set_value(hash_index, h0, "0");
    config_save_in_file(hash_index, path_hash_index);
    config_destroy(hash_index);
    free(h0);


    // 2) Reescribir metadata del initial_file:BASE a COMMITED

    FILE* f_meta = fopen(path_metadata, "w");
    if (!f_meta) {
        perror("No se pudo abrir metadata.config para escritura (commit base)");
        exit(EXIT_FAILURE);
    }
    fprintf(f_meta, "TAMAÑO=%d\nBLOCKS=[0]\nESTADO=COMMITED\n", BLOCK_SIZE);
    fclose(f_meta);


    log_debug_seguro(storage_logger, "File System formateado correctamente.");
}




void verificar_initial_file_base() {

    char path_tag_base[PATH_MAX];
    char path_metadata[PATH_MAX];
    char path_logical_block[PATH_MAX];
    char path_block_fisico[PATH_MAX];

    if (snprintf(path_tag_base, PATH_MAX, "%s/initial_file/BASE", path_files) >= PATH_MAX ||
        snprintf(path_metadata, PATH_MAX, "%s/metadata.config", path_tag_base) >= PATH_MAX ||
        snprintf(path_logical_block, PATH_MAX, "%s/logical_blocks/000000.dat", path_tag_base) >= PATH_MAX ||
        snprintf(path_block_fisico, PATH_MAX, "%s/block0000.dat", path_blocks) >= PATH_MAX) {
        log_error_seguro(storage_logger, "Path demasiado largo en verificación de initial_file:BASE");
        exit(EXIT_FAILURE);
    }

    // Verificar existencia de directorio y metadata
    if (access(path_tag_base, F_OK) == -1 || access(path_metadata, F_OK) == -1) {
        log_error_seguro(storage_logger, "Falta initial_file:BASE o su metadata.config");
        exit(EXIT_FAILURE);
    }

    // Verificar contenido de metadata
    t_config* metadata_config = config_create(path_metadata);
    if (!metadata_config) {
        log_error_seguro(storage_logger, "No se pudo leer metadata.config de initial_file:BASE");
        exit(EXIT_FAILURE);
    }

    // OJO: estado no se libera manualmente, lo libera config_destroy
    char* estado = config_get_string_value(metadata_config, "ESTADO");
    int tamanio = config_get_int_value(metadata_config, "TAMAÑO");

    // bloques SÍ debe liberarse manualmente
    char** bloques = config_get_array_value(metadata_config, "BLOCKS");

    // Validaciones robustas: chequeá punteros antes de usarlos
    if (!estado || (strcmp(estado, "WORK_IN_PROGRESS") != 0 && strcmp(estado, "COMMITED") != 0)) {
        log_error_seguro(storage_logger, "Estado inválido en metadata de initial_file:BASE");
        free_string_array(bloques);         // liberar si fue pedido
        config_destroy(metadata_config);
        exit(EXIT_FAILURE);
    }

    // Chequeo de tamaños y array
    // - tamanio debe ser exactamente BLOCK_SIZE
    // - bloques debe existir
    // - bloques[0] debe existir y ser "0"
    // - bloques[1] debe ser NULL (solo un bloque lógico)
    if (tamanio != BLOCK_SIZE || !bloques || !bloques[0] || strcmp(bloques[0], "0") != 0 || bloques[1] != NULL) {
        log_error_seguro(storage_logger, "Metadata de initial_file:BASE no tiene bloque físico 0 asignado (o lista inválida)");
        free_string_array(bloques);
        config_destroy(metadata_config);
        exit(EXIT_FAILURE);
    }

    // Ya no se necesita el array: liberalo antes de destruir la config
    free_string_array(bloques);
    config_destroy(metadata_config);

    // Verificar existencia del hard link
    struct stat stat_logico, stat_fisico;
    if (stat(path_logical_block, &stat_logico) == -1 || stat(path_block_fisico, &stat_fisico) == -1) {
        log_error_seguro(storage_logger, "Falta bloque lógico o físico en initial_file:BASE");
        exit(EXIT_FAILURE);
    }

    if (stat_logico.st_ino != stat_fisico.st_ino) {
        log_error_seguro(storage_logger, "El bloque lógico 000000.dat no es hard link al bloque físico 0");
        exit(EXIT_FAILURE);
    }

    // Verificar que el bloque físico 0 esté marcado como ocupado
    if (!bitarray_test_bit(bitmap, 0)) {
        log_error_seguro(storage_logger, "Bloque físico 0 no está marcado como ocupado en el bitmap");
        exit(EXIT_FAILURE);
    }

    log_debug_seguro(storage_logger, "Verificación de initial_file:BASE exitosa.");
}
