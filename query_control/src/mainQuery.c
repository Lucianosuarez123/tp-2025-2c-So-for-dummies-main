#include <../includes/mainQuery.h>
int main(int argc, char* argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Uso: %s [archivo_config] [archivo_query] [prioridad] \n", argv[0]);
        return 1;
    }

    archivo_config = argv[1];
    archivo_query = argv[2];
    prioridad = atoi(argv[3]);
    inicializar_Query();

    //nos conectamos con el modulo master
    fd_master = crear_conexion_cliente(IP_MASTER, PUERTO_MASTER);
    if (fd_master!=-1){
        log_info_seguro(query_logger,"## Conexión al Master exitosa. IP: %s, Puerto: %s", IP_MASTER, PUERTO_MASTER);
    }
    pthread_t hilo_query;
    pthread_create(&hilo_query, NULL, atender_query_master, NULL);
    enviar_query_a_master(fd_master, archivo_query, prioridad);
    pthread_join(hilo_query, NULL);
    //*hay que crear un hilo para que quede escuchando mensajes del master

    return 0;
}
