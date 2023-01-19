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
    initial size for object that needs to be isolated
  Output:
    Minimal size of object that can be isolated that can contain initial size
  Example:
    For page table based isolation the size is rounded up to the physical page size
*/
size_t sandboxSizeRounding(size_t size);


/* 
  Input:
    initial size for object that needs to be isolated
  Output:
    Exponent of minimal power of two that objects need to be alligned at
  Example:
    For page table based isolation the allignment is the two logarithm of the page size
*/
unsigned char sandboxSizeAlignment(size_t size);

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
