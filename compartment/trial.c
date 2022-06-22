// include for printing
#include <stdio.h>
#include <sys/mman.h>

// #include "sys/cheri/cheric.h"

#include "compartment.h"

int main(int argc, char const *argv[]) {
  __label__ returnLabel, PCCLabel;

  // open the libarary file
  FILE* basicLib = fopen("./libbasic.so","r");
  if(!basicLib){
    printf("could not open basicLib\n");
  }
  // go to the libarary function
  int offset = 0x106d8 - 0x10644 +  0x644;
  const int instructions = 4; // 0x10 / 4
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
  int(* __capability wrappedFunction)(int);
  __asm__ volatile (
    "cvtp %x[functionCap], %x[functionPointer] \n"
    : [functionCap] "+r" (wrappedFunction)
    : [functionPointer] "r" (functionCode)
  );


  // call to it
  int testInt = 7;

  // prepare environment
  const int functionMemSize = 0;
  void* functionMemory =
    mmap(NULL, functionMemSize + 16, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
  // make capabilities
  char* __capability functionMemoryCap;
  char* __capability returnPair[2];
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile (
    "cvtd %x[memCap], %x[memPointer] \n"
    "cvtp %x[returnCap], %x[returnPointer] \n"
    : [memCap] "+r" (functionMemoryCap),
    [returnCap] "+r" (returnCap)
    : [memPointer] "r" (functionMemory),
    [returnAddress] "r" (returnAddress)
  );
  // returnCap = __builtin_cheri_program_counter_get();
  char* __capability contextSpaceCap[NUM_REGS];
  returnPair[0] = contextSpaceCap;
  returnPair[1] = returnCap;

  // store current context
  storeContext(contextSpaceCap);
  // seal and store context capability and return capability in function mem
  __asm__ volatile (
    "seal %x[returnPair], %x[returnPair], lpb \n"
    : [returnPair] "+r" (*returnPair)
  );
  ((char* __capability * __capability) functionMemoryCap)[0] = returnPair;
  // Prepare arguments
  // TODO
  // clean up context and jump
  // TODO
  __asm__ volatile (
    "msr ddc, %x[memCap] \n"
    : : [memCap] "r" (functionMemoryCap)
  );

  testInt = wrappedFunction(testInt);

  // read arguments

  returnLabel:
  // restore context
  restoreContext(contextSpaceCap);
  // read out results

  printf("test_move is now: %d \n", testInt);

  return 0;
}
