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

  // prepare allocations
  const int capSize = sizeof(void*__capability);
  const int argumentSize = ((2*sizeof(int)+capSize-1)/capSize)*capSize;
  const size_t functionMemSize = argumentSize + capSize;
  char* functionMemory = malloc(functionMemSize);
  void* stackPointer = (void*)functionMemSize;

  uint64_t start, end;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(start));
  for(int repetitions = 0; repetitions < 1; repetitions++){

  // Prepare arguments
  int* argstart = (int*) functionMemory + sizeof(char* __capability);
  argstart[0] = testInt;

  printf("functionMemory  %p\n", functionMemory);
  printf("stack  %p\n", stackPointer);

  // perform function call
  sandboxedCall(trialFunction, 28, 0,
    functionMemory, functionMemSize, 0,
    stackPointer);

  // read out results
  testInt = argstart[0];
  }
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(end));

  // free memory
  free(functionMemory);

  printf("start %lu\n", start);
  printf("end %lu\n", end);
  printf("time %lu\n", end-start);

  printf("test_move is now: %d \n", testInt);

  return 0;
}
