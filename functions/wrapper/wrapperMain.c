// System Headers
#include <stdio.h>
#include <stdint.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "dandelionIO.h"

// int test_initialized = 5;
// int test_uninitialized;
// int test_inFunc = 6;

// void testFunc(void){
//   test_inFunc = 9;
// }

int main(void) {
  // test_initialized = 7;
  // test_uninitialized = 8;
  // testFunc();
  // int outputNumber = 11* (*inputPointer);

  size_t dimSize = getInputSize(0);
  int matDim = *(int*)getInputPointer(0); 
  size_t matSize = getInputSize(1);
  int64_t* inMat = getInputPointer(1);

  // slight cheat for now, as we don't have malloc yet.
  int64_t * outMat = (int64_t*)((char*)inMat + matSize);

  addOutput();
  setOutputSize(0, matSize);
  setOutputPointer(0, outMat);

  for(int i = 0; i < matDim; i++){
    for(int j = 0; j < matDim; j++){
      for(int k = 0; k < matDim; k++){
        outMat[i*matDim + j] += inMat[i*matDim + k] * inMat[j*matDim + k];
      }
    }
  }

  function_exit();
}
