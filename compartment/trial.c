// include for printing
#include <stdio.h>
#include <sys/mman.h>

#include "cheri/cheri.h"

#include "compartment.h"

int main(int argc, char const *argv[]) {

  // open the libarary file
  FILE* basicLib = fopen("./libbasic.so","r");
  if(!basicLib){
    printf("could not open basicLib\n");
  }
  // go to the libarary function
  int offset = 0x10670 - 0x105f4 +  0x5f4;
  const int instructions = 6; // 0x18 / 4
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
  int(* __capability addOne)(int);
  __asm__ volatile (
    "cvtp %x[functionCap], %x[functionPointer] \n"
    : [functionCap] "+r" (addOne)
    : [functionPointer] "r" (functionCode)
  );


  // call to it
  int testInt = 7;

  // prepare environment
  void* functionMemory =
    mmap(NULL, 16, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
  void* contextSpace =
    mmap(NULL, NUM_REGS*16, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
  // make capabilities
  void* __capability functionMemoryCap;
  void* __capability contextSpaceCap;
  __asm__ volatile (
    "cvtd %x[memCap], %x[memPointer] \n"
    "cvtd %x[contextCap], %x[contextPointer] \n"
    : [memCap] "+r" (functionMemoryCap),
    [contextCap] "+r" (contextSpaceCap)
    : [memPointer] "r" (functionMemory),
    [contextPointer] "r" (contextSpace)
    : "c0"
  );
  // store current context
  storeContext(contextSpaceCap);
  // seal and store context capability and return capability in function mem
  // TODO
  // Prepare aguments
  // TODO
  // clean up context and jump
  // TODO

  testInt = addOne(testInt);

  // restore context
  restoreContext(contextSpaceCap);
  // read out results

  printf("test_move is now: %d \n", testInt);

  return 0;
}
