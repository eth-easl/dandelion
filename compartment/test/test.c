// System Headers
#include <stdlib.h>
// Standard Libraries

// Project External Libraries
#include "unity.h"
// Project Internal Libraries
#include "compartment.h"

// test function definitions
extern void overwriteAll(void);

// functions to call before and after each test
void setUp(void){}
void tearDown(void){}

void testCapabiltyEquality(
  void* __capability expected,
  void* __capability actual
)
{
  TEST_ASSERT_EQUAL_INT64_MESSAGE(
    __builtin_cheri_tag_get(expected),
    __builtin_cheri_tag_get(actual),
     "capability tags don't match"
  );
  TEST_ASSERT_EQUAL_INT64_MESSAGE(
    __builtin_cheri_sealed_get(expected),
    __builtin_cheri_sealed_get(actual),
    "capability sealed doesn't match"
  );
  TEST_ASSERT_EQUAL_INT64_MESSAGE(
    __builtin_cheri_type_get(expected),
    __builtin_cheri_type_get(actual),
     "capability types don't match"
  );
  TEST_ASSERT_EQUAL_HEX64_MESSAGE(
    __builtin_cheri_address_get(expected),
    __builtin_cheri_address_get(actual),
     "capability addresses don't match"
  );
  TEST_ASSERT_EQUAL_INT64_MESSAGE(
    __builtin_cheri_base_get(expected),
    __builtin_cheri_base_get(actual),
     "capability bases don't match"
  );
  TEST_ASSERT_EQUAL_INT64_MESSAGE(
    __builtin_cheri_length_get(expected),
    __builtin_cheri_length_get(actual),
     "capability lengths don't match"
  );
}

void testStackCapability(void){
  void* __capability wrappedOverwrite = wrapCode(overwriteAll, 39);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(sizeof(void* __capability));

  void* __capability stackCap;
  __asm__ volatile(
    "cpy %x[stackCap], CSP \n" : [stackCap] "=r" (stackCap)
  );

  sandboxedCall(wrappedOverwrite, functionMemoryCap);

  void* __capability stackCapAfter;
  __asm__ volatile(
    "cpy %x[stackCap], CSP \n" : [stackCap] "=r" (stackCapAfter)
  );

  testCapabiltyEquality(stackCap, stackCapAfter);
}

void testDDC(void){
  void* __capability wrappedNoOp = wrapCode(overwriteAll, 39);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(sizeof(void* __capability));

  void* __capability defaultCap;
  __asm__ volatile(
    "mrs %x[defaultCap], DDC \n" : [defaultCap] "=r" (defaultCap)
  );

  sandboxedCall(wrappedNoOp, functionMemoryCap);

  void* __capability defaultCapAfter;

  __asm__ volatile(
    "mrs %x[defaultCap], DDC \n" : [defaultCap] "=r" (defaultCapAfter)
  );

  testCapabiltyEquality(defaultCap, defaultCapAfter);
}

void testCompartmentId(void){
  void* __capability wrappedNoOp = wrapCode(overwriteAll, 39);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(sizeof(void* __capability));

  void* __capability compartementId;
  __asm__ volatile(
    "mrs %x[CID], CID_EL0 \n" : [CID] "=r" (compartementId)
  );

  sandboxedCall(wrappedNoOp, functionMemoryCap);

  void* __capability compartementIdAfter;
  __asm__ volatile(
    "mrs %x[CID], CID_EL0 \n" : [CID] "=r" (compartementIdAfter)
  );

  testCapabiltyEquality(compartementId, compartementIdAfter);
}

void testThreadId(void){
  void* __capability wrappedNoOp = wrapCode(overwriteAll, 39);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(sizeof(void* __capability));

  void* __capability threadId;
  __asm__ volatile(
    "mrs %x[threadId], CTPIDR_EL0 \n" : [threadId] "=r" (threadId)
  );

  sandboxedCall(wrappedNoOp, functionMemoryCap);

  void* __capability threadIdAfter;
  __asm__ volatile(
    "mrs %x[threadId], CTPIDR_EL0 \n" : [threadId] "=r" (threadIdAfter)
  );

  testCapabiltyEquality(threadId, threadIdAfter);
}

void testRestrictedThreadId(void){
  void* __capability wrappedNoOp = wrapCode(overwriteAll, 39);
  char* __capability functionMemoryCap =
  (__cheri_tocap char* __capability) malloc(sizeof(void* __capability));

  void* __capability restrictedId;
  __asm__ volatile(
    "mrs %x[RID], RCTPIDR_EL0 \n" : [RID] "=r" (restrictedId)
  );

  sandboxedCall(wrappedNoOp, functionMemoryCap);

  void* __capability restrictedIdAfter;
  __asm__ volatile(
    "mrs %x[RID], RCTPIDR_EL0 \n" : [RID] "=r" (restrictedIdAfter)
  );

  testCapabiltyEquality(restrictedId, restrictedIdAfter);
}

int main(int argc, char const *argv[]) {
  UNITY_BEGIN();
  // test for EL0 system registers
  RUN_TEST(testStackCapability);
  RUN_TEST(testDDC);
  RUN_TEST(testCompartmentId);
  RUN_TEST(testThreadId);
  RUN_TEST(testRestrictedThreadId);
  return UNITY_END();
}
