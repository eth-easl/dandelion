#ifndef C_MACHINE_LIBRARIES_CHERI_MEMORY_H__
#define C_MACHINE_LIBRARIES_CHERI_MEMORY_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct {
  char* __capability cap;
  size_t size;
} cheri_context;

// allocation functions
cheri_context* cheri_alloc(size_t size);
void cheri_free(cheri_context* context, size_t size);

// interaction functions
void cheri_write_context(cheri_context* context, unsigned char* source_pointer,
                         size_t context_offset, size_t size);
void cheri_read_context(cheri_context* context,
                        unsigned char* destination_pointer,
                        size_t context_offset, size_t size, char sanitize);
#endif