#ifndef CODIGOS_OPERACION_H_
#define CODIGOS_OPERACION_H_

//mensaje de control
#define DESCONEXION -1





typedef enum
{
    //codigos de query a master
    ENVIAR_QUERY_A_MASTER,//[int prioridad, char* archivo_query]

    //codigos de master a query
    LECTURA_REALIZADA, //[char* file:tag, char* contenido]
    FINALIZACION_DE_QUERY, //[char* motivo]

    //codigos de master a worker
    EJECUTAR_QUERY, //[int id_query, int pc, char* archivo_query]
    INTERRUPCION, //[in id_query]

    //codigo de worker a master y storage
    ENVIAR_ID_WORKER,//[char* ID_Worker]

    //operacion de worker a master?
    OP_END,           // [int id_query char* motivo]
    RESPUESTA_INTERRUPCION,// [int query_id, int pc]
    OP_LECTURA_REALIZADA,// [int query_id, char* file:tag, char* contenido_leido]

    //codigos de worker a storage
    OP_CREATE,        // [int id_query, char* nombre_file, char* tag]
    OP_TRUNCATE,      // [int id_query, char* nombre_file, char* tag, int tamaño]
    OP_WRITE,         // [int id_query, char* nombre_file, char* tag, int nro_pagina, char* contenido]
    OP_TAG,           // [int id_query, char* file_origen, char* tag_origen, char* file_destino, char* tag_destino]
    OP_COMMIT,        // [int id_query, char* nombre_file, char* tag]
    OP_DELETE,        // [int id_query, char* nombre_file, char* tag]
    OP_PEDIR_PAGINA,   // [int id_query, char* nombre_file, char* tag, int numero_pagina]
    //codigos de storage a worker
    ENVIAR_TAM_BLOQUE_STORAGE_WORKER, // [int block_size]
    RESPUESTA_PAGINA,//[int resultado char* contenido]

    //para saber si la operacion fue exitosa entre cualquier par de modulos
    RESPUESTA_RESULTADO,//[char* resultado] //resultado ok seria "OPERACION EXITOSA"

}op_code;



#endif
