// include for printing
#include <stdio.h>
#include <sys/mman.h>

#include <cheri.h>

void reverseByteOrder(int* instruction_start){
  char* instructionPointer = (char*) instruction_start;
  char tmp = instructionPointer[0];
  instructionPointer[0] = instructionPointer[3];
  instructionPointer[3] = tmp;
  tmp = instructionPointer[1];
  instructionPointer[1] = instructionPointer[2];
  instructionPointer[2] = tmp;
}

int main(int argc, char const *argv[]) {

  int testArray[8] = {0,1,2,3,4,5,6,7};
  long pointer = 0;
  int loaded = -1;
  int moved = -1;
  int arraySample = testArray[2];
  //
  // // turn off capability mode
  // __asm__ volatile(
  //   // Not necessary as it seems they differentiates cap / pointers using
  //   // pointer bit not instruction code. Which is not quite what they say in
  //   // the manual.
  //   // "bx 4\n" // toggle capability mode
  //   // "msr ddc, %x[capPointer] \n" // set capability as DDC
  //   // "cvtd %x[pointer], %x[capPointer]\n" // convert capability to pointer
  //   // "mov %w[input], 8\n" // normal move operation, should always work
  //   // the default data capability is set correctly
  //   // "ldr %w[loadTarget], [ %x[pointer], 4 ] \n" // load operation, should only work when capabilities are enabled or
  //   : [input] "+r" (moved),
  //   [loadTarget] "+r" (loaded),
  //   [pointer] "+r" (pointer)
  //   : [capPointer] "r" (testArray)
  //   : "c28"
  // );
  //
  // turn capability mode back on
  printf("test_move is now: %d \n", moved);
  printf("test_array is %p \n", testArray);
  printf("test_array value is %d \n", arraySample);
  printf("test_pointer is %p \n", pointer);
  printf("test_load is now: %d \n", loaded);

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

  // swap byte order
  printf("instructions: \n");
  for(int i = 0; i < instructions; i++){
    // reverseByteOrder(((int*)functionCode) + i);
    printf("%08x\n", ((int*)functionCode)[i]);
  }
  void* __capability pcc = cheri_program_counter_get();

  // check bounds on ddc and pcc
  unsigned long pccBase = 7;
  unsigned long pccLimit = 11;
  unsigned long pccLength = 13;
  unsigned long pccPerm = -1;
  unsigned long ddcBase = 7;
  unsigned long ddcLimit = 11;
  unsigned long ddcLength = 13;
  unsigned long ddcPerm = -1;
  __asm__ volatile(
    "gcbase %x[pccBase], %w[pcc] \n"
    "gclim %x[pccLimit], %w[pcc] \n"
    "gclen %x[pccLength], %w[pcc] \n"
    "gcperm %x[pccPerm], %w[pcc] \n"
    "mrs c0, ddc \n"
    "gcbase %x[ddcBase], c0 \n"
    "gclim %x[ddcLimit], c0 \n"
    "gclen %x[ddcLength], c0 \n"
    "gcperm %x[ddcPerm], c0 \n"
    : [pccBase] "+r" (pccBase),
    [pccLimit] "+r" (pccLimit),
    [pccLength] "+r" (pccLength),
    [pccPerm] "+r" (pccPerm),
    [ddcBase] "+r" (ddcBase),
    [ddcLimit] "+r" (ddcLimit),
    [ddcLength] "+r" (ddcLength),
    [ddcPerm] "+r" (ddcPerm)
    : [pcc] "r" (pcc) : "c0"
  );

  printf("PCC base \t %ld\n", pccBase);
  printf("PCC limit \t %ld\n", pccLimit);
  printf("PCC length \t %ld\n", pccLength);
  printf("PCC perm \t %ld\n", pccPerm);
  printf("DDC base \t %ld\n", ddcBase);
  printf("DDC limit \t %ld\n", ddcLimit);
  printf("DDC length \t %ld\n", ddcLength);
  printf("DDC perm \t %ld\n", ddcPerm);

  // make a function pointer from it.
  int(*addOne)(int) = (int(*)(int)) functionCode;

  // call to it
  moved = addOne(moved);

  printf("test_move is now: %d \n", moved);

  return 0;
}
