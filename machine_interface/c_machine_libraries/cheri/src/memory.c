// Implementing
#include "memory.h"
// System Headers

// Standard Libraries
#include <string.h>
#include <sys/mman.h>
// External Libraries

// Internal Libraries

// TODO? optimize
unsigned char sandbox_size_alignment(size_t size) {
  size_t mask;
  __asm__ volatile("RRMASK %0, %1" : "+r"(mask) : "r"(size));
  int allignment = 63;
  while (allignment > 0) {
    if (!((1L << allignment) & mask)) {
      break;
    }
    allignment--;
  }
  // gives the left shift to first non 0, this means last 1 is +2
  // 1 for correcting that we detect the first 0
  // and 1 for accounting the original 1
  allignment += 2;
  return allignment;
}

cheri_context *cheri_alloc(size_t size) {
  // setup configure size and allignment
  size_t allocation_size = sandbox_size_rounding(size);
  // check if when rounded to representable lenght is still larger then
  // requested
  if (allocation_size < size) return NULL;
  int allignment = sandbox_size_alignment(allocation_size);
  char *context_space_addr =
      mmap(NULL, size, PROT_EXEC | PROT_READ | PROT_WRITE,
           MAP_ANONYMOUS | MAP_ALIGNED(allignment), -1, 0);
  if (context_space_addr == MAP_FAILED) return NULL;
  char *__capability context_space =
      (__cheri_tocap char *__capability)context_space_addr;
  context_space = __builtin_cheri_bounds_set(context_space, allocation_size);
  // adjust permissions
  const size_t permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(context_space)
                   : "r"(permission_mask));
  cheri_context *context = malloc(sizeof(cheri_context));
  context->size = allocation_size;
  context->cap = context_space;
  return context;
}

void cheri_free(cheri_context *context, size_t size) {
  //   todo error handling
  munmap((__cheri_fromcap void *)context->cap, size);
  free(context);
}

void cheri_write_context(cheri_context *context, unsigned char *source_pointer,
                         size_t context_offset, size_t size) {
  void *dst = (__cheri_fromcap void *)context->cap + context_offset;
  memcpy(dst, source_pointer, size);
}

void cheri_read_context(cheri_context *context,
                        unsigned char *destination_pointer,
                        size_t context_offset, size_t size, char sanitize) {
  void *src = (__cheri_fromcap void *)context->cap + context_offset;
  memcpy(destination_pointer, src, size);
  if (sanitize != 0) {
    memset(src, 0, size);
  }
}