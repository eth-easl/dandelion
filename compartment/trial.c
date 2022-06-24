// include for printing
#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>

#include "compartment.h"

int main(int argc, char const *argv[]) {
  __label__ returnLabel;

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

  // printf("instructions: \n");
  // for(int i = 0; i < instructions; i++){
  //   printf("%08x\n", ((int*)functionCode)[i]);
  // }

  // make a function pointer from it.
  void(* __capability wrappedFunction)(void);
  __asm__ volatile (
    "cvtp %x[functionCap], %x[functionPointer] \n"
    : [functionCap] "+r" (wrappedFunction)
    : [functionPointer] "r" (functionCode)
  );


  // call to it
  int testInt = 7;

  // prepare environment
  const int argumentSize = sizeof(int);
  const int functionMemSize = argumentSize + sizeof(char* __capability);
  void* functionMemory =
    mmap(NULL, functionMemSize, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
  // make capabilities
  char* __capability functionMemoryCap;
  char* __capability * __capability returnPair =
    (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*2);
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile (
    "cvtd %x[memCap], %x[memPointer] \n"
    "cvtp %x[returnCap], %x[returnAddress] \n"
    : [memCap] "+r" (functionMemoryCap),
    [returnCap] "+r" (returnCap)
    : [memPointer] "r" (functionMemory),
    [returnAddress] "r" (returnAddress)
  );

  char* __capability * __capability contextSpaceCap =
    (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*NUM_REGS);

  returnPair[0] = (char* __capability) contextSpaceCap;
  returnPair[1] = returnCap;

  // seal and store context capability and return capability in function mem
  __asm__ volatile (
    "seal %x[returnPair], %x[returnPair], lpb \n"
    : [returnPair] "+r" (returnPair)
  );

  ((char* __capability * __capability) functionMemoryCap)[0] = (char* __capability) returnPair;
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

  printf("test_move is now: %d \n", testInt);

  return 0;
}
