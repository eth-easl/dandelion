// System Headers
#include <stdio.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "dandelionIO.h"

int test_initialized = 5;
int test_uninitialized;
int test_inFunc = 6;
void* debugSymbol = NULL;
long int debugSymbol2 = 2;

void testFunc(void){
  test_inFunc = 9;
}

int main(void) {
  test_initialized = 7;
  test_uninitialized = 8;
  testFunc();

  size_t inputSize = getInputSize(0);
  int* inputPointer = getInputPointer(0);

  addOutput();
  int outPutNumber = 11* (*inputPointer);
  debugSymbol = &outPutNumber;
  debugSymbol2 = outPutNumber;
  setOutputSize(0, 4);
  setOutputPointer(0, &outPutNumber);

  // printf("%d\n", test_initialized);
  // printf("%d\n", test_uninitialized);
  // printf("%d\n", test_inFunc);
  // return 7;

  __asm__ volatile(
    "mrs c0, ddc \n"
    "ldr c0, [c0] \n"
    "ldpbr c29, [c0] \n"
  );
  __builtin_unreachable();
}
