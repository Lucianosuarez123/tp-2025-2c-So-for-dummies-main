 #include <../includes/globalStorage.h>

void free_string_array(char** arr) {
    if (!arr) return;
    for (int i = 0; arr[i] != NULL; i++) {
        free(arr[i]);
    }
    free(arr);
}


