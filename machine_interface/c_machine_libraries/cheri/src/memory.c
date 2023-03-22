#include "memory.h"

cheri_cap* cheri_alloc(size_t size){
    return malloc(sizeof(cheri_cap));
}

void cheri_free(cheri_cap* cap){
    free(cap);
}