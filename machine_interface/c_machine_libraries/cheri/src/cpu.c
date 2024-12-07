#include "cpu.h"

char cheri_run_static(unsigned char* cap_start, size_t cap_size, size_t entry_point,
                      size_t return_pair_offset, size_t stack_pointer) {
//   void* __capability context_cap = context->cap;
  // create capability 
  // set up capability for code
//   unsigned long start_pointer = __builtin_cheri_base_get(context_cap);
//   unsigned long size = __builtin_cheri_length_get(context_cap);
  char* __capability data = (__cheri_tocap char* __capability) (char*)cap_start;
  data = __builtin_cheri_bounds_set(data, cap_size);
  // adjust permissions
  const size_t permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(data)
                   : "r"(permission_mask));
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, cap_start);
  pcc = __builtin_cheri_bounds_set(pcc, cap_size);
  pcc = pcc + entry_point;
  // restrict capability to only be fetch,
  // and executive(executive could be removed in future)
  const unsigned long pccPermissionMask = ~(code_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(pcc) : "r"(pccPermissionMask));
  return cheri_execute(data, pcc, return_pair_offset, (void*)stack_pointer);
}

char cheri_execute(char* __capability memory, void* __capability function,
                   size_t return_pair_offset, void* stack_pointer) {
  __label__ returnLabel;
  // allocate space for context
  char* __capability* contextSpace =
      malloc(sizeof(char* __capability) * NUM_REGS);
  if (contextSpace == NULL) return MALLOC_ERROR;
  char* __capability* __capability contextSpaceCap =
      (__cheri_tocap char* __capability* __capability)contextSpace;

  // allocate return pair
  char* __capability* returnPair = malloc(sizeof(char* __capability) * 2);
  if (returnPair == NULL) return MALLOC_ERROR;
  char* __capability* __capability returnPairCap =
      (__cheri_tocap char* __capability* __capability)returnPair;

  // make a return capability
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile("cvtp %x[returnCap], %x[returnAddress] \n"
                   : [returnCap] "+r"(returnCap)
                   : [returnAddress] "r"(returnAddress));

  // prepare and seal return pair
  returnPairCap[0] = (char* __capability)contextSpaceCap;
  returnPairCap[1] = returnCap;
  __asm__ volatile("seal %x[returnPairCap], %x[returnPairCap], lpb \n"
                   : [returnPairCap] "+r"(returnPairCap));
  char* memory_pointer = (__cheri_fromcap char*)memory;
  *((char* __capability*)(memory_pointer + return_pair_offset)) =
      (char* __capability)returnPairCap;
  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(memory, stack_pointer, function);

returnLabel:
  // restore context needs context cap in c0.
  restoreContext();

  free(returnPair);
  free(contextSpace);

  return SUCCESS;
}