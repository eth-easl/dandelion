#ifndef C_MACHINE_LIBRARIES_CHERI_MEMORY_H__
#define C_MACHINE_LIBRARIES_CHERI_MEMORY_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct {
    uint8_t cap[16];
} cheri_cap;

cheri_cap* cheri_alloc(size_t size);
void cheri_free(cheri_cap* cap);

#endif