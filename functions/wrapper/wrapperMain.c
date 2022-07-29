// System Headers
#include <stdio.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
// #include "dandelionIO.h"

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
  printf("%d\n", test_initialized);
  printf("%d\n", test_uninitialized);
  printf("%d\n", test_inFunc);
  return 0;
}
