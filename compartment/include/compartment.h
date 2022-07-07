#ifndef DANDELION_COMPARTMENT_COMPARTMENT_H_
#define DANDELION_COMPARTMENT_COMPARTMENT_H_

// Input:
// functionCode: function code to be jumped to, expect that function uses
// first value stored into function memory as a sealed return pair
// fuctionMemory: memory to be handed over to the function, a sealed return
// pair will be stored at the 0 offset to the pointer.
void sandboxedCall(
    void* __capability functionCode,
    char* __capability functionMemory
  );

#endif // DANDELION_COMPARTMENT_COMPARTMENT_H_
