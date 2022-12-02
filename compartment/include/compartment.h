#ifndef DANDELION_COMPARTMENT_COMPARTMENT_H_
#define DANDELION_COMPARTMENT_COMPARTMENT_H_

#include <stdlib.h>

/*
  Input:
    functionCode: pointer to the start of the code to be wrapped
    size: number of 32-bit instructions belonging to the function
  Output:
    Capability pointing to executable code that is wrapped with the sealed pair
    return
*/
void* __capability wrapCode(void* functionCode, int size);

/*
Input:
  functionCode: function code to be jumped to, expect that function uses
    first value stored into function memory as a sealed return pair
  fuctionMemory: memory to be handed over to the function, a sealed return
    pair will be stored at the 0 offset to the pointer.
*/
void sandboxedCall(
    void* functionCode,
    size_t codeSize,
    size_t entryPointOffset,
    char* functionMemory,
    size_t memorySize,
    size_t returnPairOffset,
    void* functionStackPointer
  );

#endif // DANDELION_COMPARTMENT_COMPARTMENT_H_
