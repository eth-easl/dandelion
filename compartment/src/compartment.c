// Implementing
#include "compartment.h"
// System Headers
#include <sys/mman.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "context.h"

// assembly import
#define WRAPPER_SIZE 5
#define WRAPPER_BYTES (WRAPPER_SIZE * 4)
extern void wrapperCode();

// cheri permissions
static const int codePermissions =    0x08002;
static const int memoryPermissions =  0x34000;

void* __capability wrapCode(void* functionCode, int size){
  // get memory to copy to
  int* wrappedCode = (int*) mmap(NULL,
    WRAPPER_BYTES + size,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );
  // write wrapper at the start
  for(int i = 0; i < WRAPPER_SIZE; i++){
    wrappedCode[i] = ((int*)wrapperCode)[i];
  }
  // overwrite wrapper return
  // TODO make sure capabilities are copied properly if we allow capabilities to be there
  for(int i = 0; i < size; i++){
    wrappedCode[WRAPPER_SIZE-1 + i] = ((int*)functionCode)[i];
  }
  void(* __capability wrappedFunction)(void);
  __asm__ volatile (
    "cvtp %x[functionCap], %x[functionPointer] \n"
    : [functionCap] "+r" (wrappedFunction)
    : [functionPointer] "r" (wrappedCode)
  );

  return wrappedFunction;
}

size_t sandboxSizeRounding(size_t size){
  __asm__ volatile ("RRLEN %0, %0" : "+r"(size));
  return size;
}
// TODO optimize
unsigned char sandboxSizeAlignment(size_t size){
  size_t mask;
  __asm__ volatile("RRMASK %0, %1" : "+r"(mask) : "r"(size));
  int allignment = 63;
  while(allignment > 0){
    if(!((1L << allignment) & mask)){
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

void sandboxedCall(
  void* functionCode,
  size_t codeSize,
  size_t entryPointOffset,
  char* functionMemory,
  size_t memorySize,
  size_t returnPairOffset,
  void* functionStackPointer
){
  __label__ returnLabel;
  // TODO debug checks for proper alignment
  
  // set up capability for code
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionCode);
  pcc = __builtin_cheri_bounds_set(pcc, codeSize);
  pcc = pcc + entryPointOffset;
  // restrict capability to only be fetch, and executive (executive could be removed in future)
  const unsigned long pccPermissionMask = ~(codePermissions);
  __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(pcc) : "r"(pccPermissionMask));
  // set up capability for memory
  void* __capability ddc = (__cheri_tocap void*__capability) functionMemory;
  ddc = __builtin_cheri_bounds_set(ddc, memorySize);
  const unsigned long ddcPermissionMask = ~(memoryPermissions);
  __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(ddc) : "r"(ddcPermissionMask));
  // allocate space for context
  char* __capability * __capability contextSpaceCap =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*NUM_REGS);

  // allocate return pair
  char* __capability * __capability returnPair =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*2);

  // make a return capability
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile (
    "cvtp %x[returnCap], %x[returnAddress] \n"
    : [returnCap] "+r" (returnCap)
    : [returnAddress] "r" (returnAddress)
  );

  // prepare and seal return pair
  returnPair[0] = (char* __capability) contextSpaceCap;
  returnPair[1] = returnCap;
  __asm__ volatile (
    "seal %x[returnPair], %x[returnPair], lpb \n"
    : [returnPair] "+r" (returnPair)
  );
  *((char* __capability *) (functionMemory + returnPairOffset))
    = (char* __capability) returnPair;

  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(ddc, functionStackPointer, pcc);

  returnLabel:
  // restore context needs context cap in c0.
  restoreContext();

  free((__cheri_fromcap void*) returnPair);
  free((__cheri_fromcap void*) contextSpaceCap);

  return;
}
