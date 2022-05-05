// include for printing
#include <stdio.h>

int main(int argc, char const *argv[]) {

  int testArray[8] = {0,1,2,3,4,5,6,7};
  long pointer = 0;
  int loaded = -1;
  int moved = -1;
  int arraySample = testArray[2];

  // turn off capability mode
  __asm__ volatile(
    // Not necessary as it seems they differentiates cap / pointers using
    // pointer bit not instruction code. Which is not quite what they say in
    // the manual.
    // "bx 4\n" // toggle capability mode
    "msr ddc, %x[capPointer] \n" // set capability as DDC
    "cvtd %x[pointer], %x[capPointer]\n" // convert capability to pointer
    "mov %w[input], 8\n" // normal move operation, should always work
    // the default data capability is set correctly
    "ldr %w[loadTarget], [ %x[pointer], 4 ] \n" // load operation, should only work when capabilities are enabled or
    : [input] "+r" (moved),
    [loadTarget] "+r" (loaded),
    [pointer] "+r" (pointer)
    : [capPointer] "r" (testArray)
    : "c28"
  );

  // turn capability mode back on
  printf("test_move is now: %d \n", moved);
  printf("test_array is %p \n", testArray);
  printf("test_array value is %d \n", arraySample);
  printf("test_pointer is %p \n", pointer);
  printf("test_load is now: %d \n", loaded);
  return 0;
}
