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
  size_t map_size = allocation_size;
  // check if when rounded to representable lenght is still larger then
  // requested
  if (allocation_size < size) return NULL;
  int allignment = sandbox_size_alignment(allocation_size);
  // round up to at least page alligmnent
  allignment = allignment > 12 ? allignment : 12;
#ifdef __FreeBSD__
  char *context_space_addr =
      mmap(NULL, allocation_size, PROT_EXEC | PROT_READ | PROT_WRITE,
           MAP_ANONYMOUS | MAP_ALIGNED(allignment), -1, 0);
#elif defined __linux__
  // linux mmap does not allow for allignment requirements.
  // therefore allocate double the space and alligne inside
  size_t extra_space = 1LL << allignment;
  char *context_space_addr =
      mmap(NULL, allocation_size + extra_space,
           PROT_EXEC | PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
  // round it up to next fitting allginment
  size_t mask = ~(-1LL << allignment);
  char *allocation = context_space_addr;
  context_space_addr = (char *)(((uintptr_t)context_space_addr + mask) & ~mask);
  // unmap unused space
  size_t unused_lower = context_space_addr - allocation;
  if (unused_lower > 0) {
    munmap(allocation, unused_lower);
  }
  map_size += extra_space - unused_lower;
#else
#error "Non supported OS, need a valid mmap"
#endif
  if (context_space_addr == MAP_FAILED) {
    return NULL;
  }
  char *__capability context_space =
      (__cheri_tocap char *__capability)context_space_addr;
  context_space = __builtin_cheri_bounds_set(context_space, allocation_size);
  // adjust permissions
  const size_t permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(context_space)
                   : "r"(permission_mask));
  cheri_context *context = malloc(sizeof(cheri_context));
  if (context != NULL) {
    context->size = map_size;
    context->cap = context_space;
  } else {
    munmap(context_space_addr, map_size);
  }
  return context;
}

void cheri_free(cheri_context *context) {
  //   todo error handling
  munmap((__cheri_fromcap void *)context->cap, context->size);
  free(context);
}

void cheri_write_context(cheri_context *context, unsigned char *source_pointer,
                         size_t context_offset, size_t size) {
  void *dst = (__cheri_fromcap void *)context->cap + context_offset;
  memcpy(dst, source_pointer, size);
}

void cheri_read_context(cheri_context *context,
                        unsigned char *destination_pointer,
                        size_t context_offset, size_t size) {
  void *src = (__cheri_fromcap void *)context->cap + context_offset;
  memcpy(destination_pointer, src, size);
}

unsigned char* cheri_get_chunk_ref(cheri_context* context,
                        size_t context_offset){
  return ((unsigned char*)(__cheri_fromcap void*)context->cap) + context_offset;
}

void cheri_transfer_context(cheri_context *destination, cheri_context *source,
                            size_t destination_offset, size_t source_offset,
                            size_t size) {
  void *src = (__cheri_fromcap void *)source->cap + source_offset;
  void *dst = (__cheri_fromcap void *)destination->cap + destination_offset;
  memcpy(dst, src, size);
}