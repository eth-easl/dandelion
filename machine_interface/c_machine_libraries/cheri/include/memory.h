#ifndef C_MACHINE_LIBRARIES_CHERI_MEMORY_H__
#define C_MACHINE_LIBRARIES_CHERI_MEMORY_H__

#include <stdint.h>
#include <stdlib.h>

// erorr values
#define SUCCESS 0x0       // success inidication
#define MALLOC_ERROR 0x1  // for failure to allocate memory
#define NO_SETUP 0x2      // for invalid setup
#define INTERRUPTED 0x3   // interrupt was caught

int32_t is_null(void* ptr);

typedef struct {
  char* __capability cap;
  size_t size;
} cheri_context;

// cheri permissions
static const int code_permissions = 0x08002;
static const int memory_permissions = 0x34000;

// allocation functions
cheri_context* cheri_alloc(size_t size);
void cheri_free(cheri_context* context, size_t size);

// interaction functions
void cheri_write_context(cheri_context* context, unsigned char* source_pointer,
                         size_t context_offset, size_t size);
void cheri_read_context(cheri_context* context,
                        unsigned char* destination_pointer,
                        size_t context_offset, size_t size, char sanitize);
void cheri_transfer_context(cheri_context* destination, cheri_context* source,
                            size_t destination_offset, size_t source_offset,
                            size_t size, char sanitize);

// auxilliary functions for internal use
static size_t sandbox_size_rounding(size_t size) {
  __asm__ volatile("RRLEN %0, %0" : "+r"(size));
  return size;
}

#endif