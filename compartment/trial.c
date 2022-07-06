// system includes
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

#include "compartment.h"

int main(int argc, char const *argv[]) {
  __label__ returnLabel;

  // test arguments
  int testInt = 7;

  // open the libarary file
  FILE* basicLib = fopen("./libbasic.so","r");
  if(!basicLib){
    printf("could not open basicLib\n");
  }
  // go to the libarary function
  int offset = 0x106d8 - 0x10644 +  0x644;
  const int instructions = 7; // 0x1c / 4
  if(fseek(basicLib, offset, SEEK_SET)){
    printf("There was an error with seeking the libarary\n");
  }

  // get some readable, writable and executable memory
  void* functionCode = mmap(NULL,
    instructions*4,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );

  // read the aproprate number of bytes
  fread(functionCode, 4, instructions, basicLib);

  // make a function pointer from it.
  void(* __capability wrappedFunction)(void);
  __asm__ volatile (
    "cvtp %x[functionCap], %x[functionPointer] \n"
    : [functionCap] "+r" (wrappedFunction)
    : [functionPointer] "r" (functionCode)
  );

  // make a return capability
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile (
    "cvtp %x[returnCap], %x[returnAddress] \n"
    : [returnCap] "+r" (returnCap)
    : [returnAddress] "r" (returnAddress)
  );

  // prepare allocations
  const int argumentSize = sizeof(int);
  const int functionMemSize = argumentSize + sizeof(char* __capability);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(functionMemSize);
  char* __capability * __capability returnPair =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*2);
  char* __capability * __capability contextSpaceCap =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*NUM_REGS);

  uint64_t start, end;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(start));
  for(int repetitions = 0; repetitions < 1000000; repetitions++){

  // for measurement Loop
  char* __capability * __capability returnPairCap = returnPair;

  // prepare and seal return pair
  returnPairCap[0] = (char* __capability) contextSpaceCap;
  returnPairCap[1] = returnCap;
  __asm__ volatile (
    "seal %x[returnPairCap], %x[returnPairCap], lpb \n"
    : [returnPairCap] "+r" (returnPairCap)
  );
  ((char* __capability * __capability) functionMemoryCap)[0] = (char* __capability) returnPairCap;

  // Prepare arguments
  char* __capability argstart = functionMemoryCap + sizeof(char* __capability);
  ((int* __capability)argstart)[0] = testInt;

  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(functionMemoryCap, wrappedFunction);

  returnLabel:
  // restore context needs context cap in c0.
  restoreContext();
  // read out results
  testInt = ((int* __capability)argstart)[0];
  }
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(end));

  // free memory
  free((__cheri_fromcap void*) functionMemoryCap);
  free((__cheri_fromcap void*) returnPair);
  free((__cheri_fromcap void*) contextSpaceCap);

  printf("start %lu\n", start);
  printf("end %lu\n", end);
  printf("time %lu\n", end-start);

  printf("test_move is now: %d \n", testInt);

  return 0;
}
