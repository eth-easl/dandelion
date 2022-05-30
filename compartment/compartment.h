#ifndef DANDELION_COMPARTMENT_H_
#define DANDELION_COMPARTMENT_H_

// 31 normal registers, stack pointer, ddc, compartment ID
#define NUM_REGS 32

#define STRING(arg) #arg
#define MULTISTRING(arg) STRING(arg)

struct functionEnvironment {
  // pointer or capability to the code for the function
  void* codePointer;
  // size of the code segment belonging to function
  int codeSize;
  // pointer to memory containing data that needs to be copied in to the memory
  // of the function at start (globals, capabilities to other functions, etc.)
  void* dataPointer;
  // size of the object at the data pointer
  int dataSize;
  // requested memory size for the function
  int memorySize;
};
static inline void storeContext(void* __capability storeCap)
  __attribute__((always_inline));
static inline void storeContext(void* __capability storeCap) {
  __asm__ volatile(
    // store the contents of all normal registers
    "str c0, [%x[cap], #0] \n"
    "str c1, [%x[cap], #16] \n"
    "str c2, [%x[cap], #32] \n"
    "str c3, [%x[cap], #48] \n"
    "str c4, [%x[cap], #64] \n"
    "str c5, [%x[cap], #80] \n"
    "str c6, [%x[cap], #96] \n"
    "str c7, [%x[cap], #112] \n"
    "str c8, [%x[cap], #128] \n"
    "str c9, [%x[cap], #144] \n"
    "str c10, [%x[cap], #160] \n"
    "str c11, [%x[cap], #176] \n"
    "str c12, [%x[cap], #192] \n"
    "str c13, [%x[cap], #208] \n"
    "str c14, [%x[cap], #224] \n"
    "str c15, [%x[cap], #240] \n"
    "str c16, [%x[cap], #256] \n"
    "str c17, [%x[cap], #272] \n"
    "str c18, [%x[cap], #288] \n"
    "str c19, [%x[cap], #304] \n"
    "str c20, [%x[cap], #320] \n"
    "str c21, [%x[cap], #336] \n"
    "str c22, [%x[cap], #352] \n"
    "str c23, [%x[cap], #368] \n"
    "str c24, [%x[cap], #384] \n"
    "str c25, [%x[cap], #400] \n"
    "str c26, [%x[cap], #416] \n"
    "str c27, [%x[cap], #432] \n"
    "str c28, [%x[cap], #448] \n"
    "str c29, [%x[cap], #464] \n"
    "str c30, [%x[cap], #480] \n"
    // store the stack capability
    "cpy c0, CSP \n"
    "str c0, [%x[cap], #496] \n"
    // store the ddc
    "mrs c0, DDC \n"
    "str c0, [%x[cap], #512] \n"
    // store compartment ID
    "mrs c0, CID_EL0 \n"
    "str c0, [%x[cap], #528] \n"
    // Software Thead ID
    "mrs c0, RCTPIDR_EL0 \n"
    "str c0, [%x[cap], #544] \n"
    "mrs c0, CTPIDR_EL0 \n"
    "str c0, [%x[cap], #528] \n"
    : : [cap] "r" (storeCap) : "c0"
  );
}

static inline void restoreContext(void* __capability restoreCap)
  __attribute__((always_inline));
static inline void restoreContext(void* __capability restoreCap){
    __asm__ volatile(
      // put restore capability in C0 to overwrite it last
      "cpy c0, %x[cap] \n"
      // Software Thead ID
      "ldr c1, [c0, #544] \n"
      "msr RCTPIDR_EL0, c1 \n"
      "ldr c1, [c0, #528] \n"
      "msr CTPIDR_EL0, c1 \n"
      // restore compartment ID
      "ldr c1, [c0, #528] \n"
      "msr CID_EL0, c1 \n"
      // restore the ddc
      "ldr c1, [c0, #512] \n"
      "msr DDC, c1 \n"
      // store the stack capability
      "ldr c1, [%x[cap], #496] \n"
      "cpy c1, CSP \n"

      // store the contents of all normal registers
      "str c30, [c0, #480] \n"
      "str c29, [c0, #464] \n"
      "str c28, [c0, #448] \n"
      "str c27, [c0, #432] \n"
      "str c26, [c0, #416] \n"
      "str c25, [c0, #400] \n"
      "str c24, [c0, #384] \n"
      "str c23, [c0, #368] \n"
      "str c22, [c0, #352] \n"
      "str c21, [c0, #336] \n"
      "str c20, [c0, #320] \n"
      "str c19, [c0, #304] \n"
      "str c18, [c0, #288] \n"
      "str c17, [c0, #272] \n"
      "str c16, [c0, #256] \n"
      "str c15, [c0, #240] \n"
      "str c14, [c0, #224] \n"
      "str c13, [c0, #208] \n"
      "str c12, [c0, #192] \n"
      "str c11, [c0, #176] \n"
      "str c10, [c0, #160] \n"
      "str c9, [c0, #144] \n"
      "str c8, [c0, #128] \n"
      "str c7, [c0, #112] \n"
      "str c6, [c0, #96] \n"
      "str c5, [c0, #80] \n"
      "str c4, [c0, #64] \n"
      "str c3, [c0, #48] \n"
      "str c2, [c0, #32] \n"
      "str c1, [c0, #16] \n"
      "str c0, [c0, #0] \n"
      : : [cap] "r" (restoreCap)
    );
}

#endif
