// system includes
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

#include "compartment.h"

extern void trialFunction(void);

int main(int argc, char const *argv[]) {

  // test arguments
  int testInt = 7;

  /*
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
  */

  void* __capability wrappedFunction = wrapCode(trialFunction, 5);

  // prepare allocations
  const int capSize = sizeof(void*__capability);
  const int argumentSize = ((2*sizeof(int)+capSize-1)/capSize)*capSize;
  const int functionMemSize = argumentSize + capSize;
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(functionMemSize);
  char* stackPointer =
    &(((__cheri_fromcap char*)functionMemoryCap)[functionMemSize]);

  uint64_t start, end;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(start));
  for(int repetitions = 0; repetitions < 1; repetitions++){

  // Prepare arguments
  char* __capability argstart = functionMemoryCap + sizeof(char* __capability);
  ((int* __capability)argstart)[0] = testInt;

  printf("m add %p\n", __builtin_cheri_address_get(functionMemoryCap));
  printf("m bas %p\n", __builtin_cheri_base_get(functionMemoryCap));
  printf("m len %lu\n", __builtin_cheri_length_get(functionMemoryCap));
  printf("stack  %p\n", stackPointer);

  // perform function call
  sandboxedCall(wrappedFunction, functionMemoryCap, 0, stackPointer);

  // read out results
  testInt = ((int* __capability)argstart)[0];
  }
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(end));

  // free memory
  free((__cheri_fromcap void*) functionMemoryCap);

  printf("start %lu\n", start);
  printf("end %lu\n", end);
  printf("time %lu\n", end-start);

  printf("test_move is now: %d \n", testInt);

  return 0;
}
