// Implementing
#include "compartment.h"
// System Headers
#include <stdlib.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "context.h"

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
