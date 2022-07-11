// Implementing
#include "compartment.h"
// System Headers
#include <stdlib.h>
#include <sys/mman.h>
#include <stdio.h> // TODO remove
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "context.h"

// assembly import
#define WRAPPER_SIZE 5
#define WRAPPER_BYTES (WRAPPER_SIZE * 4)
extern void wrapperCode();

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

void sandboxedCall(
  void* __capability functionCode,
  char* __capability functionMemory
){
    __label__ returnLabel;
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
  ((char* __capability * __capability) functionMemory)[0] = (char* __capability) returnPair;

  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(functionMemory, functionCode);

  returnLabel:
  // restore context needs context cap in c0.
  restoreContext();

  free((__cheri_fromcap void*) returnPair);
  free((__cheri_fromcap void*) contextSpaceCap);

  return;
}
