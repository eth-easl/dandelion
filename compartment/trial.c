// system includes
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

#include "compartment.h"

int main(int argc, char const *argv[]) {

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

  // prepare allocations
  const int argumentSize = sizeof(int);
  const int functionMemSize = argumentSize + sizeof(char* __capability);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(functionMemSize);

  uint64_t start, end;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(start));
  for(int repetitions = 0; repetitions < 1000; repetitions++){

  // Prepare arguments
  char* __capability argstart = functionMemoryCap + sizeof(char* __capability);
  ((int* __capability)argstart)[0] = testInt;

  // perform function call
  sandboxedCall(wrappedFunction, functionMemoryCap);

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
