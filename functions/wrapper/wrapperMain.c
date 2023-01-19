// System Headers
#include <stdio.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "dandelionIO.h"

int test_initialized = 5;
int test_uninitialized;
int test_inFunc = 6;

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
  int outputNumber = 11* (*inputPointer);
  setOutputSize(0, 4);
  setOutputPointer(0, &outputNumber);

  function_exit();
}
