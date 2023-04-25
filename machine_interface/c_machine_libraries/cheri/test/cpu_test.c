// System Headers
#include <stdlib.h>
#include <string.h>
// Standard Libraries

// Project External Libraries
#include "unity.h"
// Project Internal Libraries
#include "cpu.h"
#include "registerState.h"

// THIS IS SEPARATELY DEFINED AGAIN IN THE ASM!
#define SANITY_VALUE 0xDEAD

// test function definitions
extern void overwriteAll(void);
extern void overwriteAllEnd(void);
static int overwriteSize;
extern void safeAll(void);
extern void safeAllEnd(void);
static int safeAllSize;
extern void triggerSigProt(void);
extern void triggerSigProtEnd(void);
static int triggerSigProtSize;

extern char sandboxedCallWrapped(char* __capability memory,
                                 void* __capability function,
                                 size_t returnPairOffset, void* stackPointer,
                                 StatePair* regState);

// functions to call before and after each test
void setUp(void) {
  overwriteSize = overwriteAllEnd - overwriteAll + 4;
  safeAllSize = safeAllEnd - safeAll + 4;
  triggerSigProtSize = triggerSigProtEnd - triggerSigProt + 4;
  int error = cheri_setup();
  TEST_ASSERT_EQUAL_INT32_MESSAGE(0, error, "Failed setup");
}
void tearDown(void) {
  int error = cheri_tear_down();
  TEST_ASSERT_EQUAL_INT32_MESSAGE(0, error, "Failed tear down");
}

void testCapabiltyEquality(void* __capability expected,
                           void* __capability actual) {
  TEST_ASSERT_EQUAL_INT64_MESSAGE(__builtin_cheri_tag_get(expected),
                                  __builtin_cheri_tag_get(actual),
                                  "capability tags don't match");
  TEST_ASSERT_EQUAL_INT64_MESSAGE(__builtin_cheri_sealed_get(expected),
                                  __builtin_cheri_sealed_get(actual),
                                  "capability sealed doesn't match");
  TEST_ASSERT_EQUAL_INT64_MESSAGE(__builtin_cheri_type_get(expected),
                                  __builtin_cheri_type_get(actual),
                                  "capability types don't match");
  TEST_ASSERT_EQUAL_HEX64_MESSAGE(__builtin_cheri_address_get(expected),
                                  __builtin_cheri_address_get(actual),
                                  "capability addresses don't match");
  TEST_ASSERT_EQUAL_INT64_MESSAGE(__builtin_cheri_base_get(expected),
                                  __builtin_cheri_base_get(actual),
                                  "capability bases don't match");
  TEST_ASSERT_EQUAL_INT64_MESSAGE(__builtin_cheri_length_get(expected),
                                  __builtin_cheri_length_get(actual),
                                  "capability lengths don't match");
}

/*
  Returns a struct with the expected state and actual state of registers after
  sandbox entry.
  The expected state should be all 0 except for the expected ddc and sp
  The actual state should be all 0 except for the ddc, sp and c30 which,
  which contains a register return address.
*/
StatePair getSandboxEntryState(void) {
  const size_t capSize = sizeof(void* __capability);
  // round up to next 16 bytes
  // add in two cap sizes, 1 for return pair, one for sanity value
  int allocSize =
      ((capSize * 2 + sizeof(RegisterState) + 1) / capSize) * capSize;
  char* functionMemory = malloc(allocSize);
  // normally stackpointer should be set at end, but set it at beginning because
  // the cap storing using the stack pointer register only works with positive
  // offsets
  char* stackPointer = functionMemory + capSize;
  // make memory cap
  char* __capability memory_cap =
      (__cheri_tocap char* __capability)functionMemory;
  memory_cap =
      __builtin_cheri_address_set(memory_cap, (unsigned long)functionMemory);
  memory_cap = __builtin_cheri_bounds_set(memory_cap, allocSize);
  const unsigned long memory_permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(memory_cap)
                   : "r"(memory_permission_mask));
  // make function cap
  void* __capability function_cap = __builtin_cheri_program_counter_get();
  function_cap =
      __builtin_cheri_address_set(function_cap, (unsigned long)safeAll);
  function_cap = __builtin_cheri_bounds_set(function_cap, safeAllSize);
  const unsigned long function_permission_mask = ~(code_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(function_cap)
                   : "r"(function_permission_mask));
  char err = cheri_execute(memory_cap, function_cap, 0, (void*)capSize);
  TEST_ASSERT_EQUAL_INT8_MESSAGE(0, err, "Execute call returned error");

  // copy values from stackpointer onward into registerState
  StatePair regState = {};
  memcpy(&regState.actual, stackPointer, sizeof(RegisterState));
  *((void**)(&regState.expected.csp)) = (void*)capSize;
  void* __capability ddc = (__cheri_tocap void* __capability)functionMemory;
  ddc = __builtin_cheri_bounds_set(ddc, allocSize);
  // clear tag, since the safing also needs to do that, because cap stores are
  // not allowed
  ddc = __builtin_cheri_tag_clear(ddc);
  regState.expected.ddc = ddc;

  // check the function actually ran
  int sanityValue = *(int*)(functionMemory + allocSize - 4);
  TEST_ASSERT_EQUAL_INT32_MESSAGE(
      SANITY_VALUE, sanityValue,
      "Sanity value missmatch, function may not have run");

  free(functionMemory);

  return regState;
}

/*
 Returns a struct with a pair of register states
 The expected pair is captured on entry by a shim that gets the pointer to
 the pair as additional argument.
 c19 and c20 are restored by the shim, as it is a callee-saved register and
 used to store the pointer to the pair and therefore always should be correct.
 given the Arm procedure call conventions not all actual registers need to
 be equal to the expected for the sandbox to be correct.
*/
StatePair getSandboxExitState(void) {
  int alloc_size = sizeof(void* __capability) * 2;
  char* functionMemory = malloc(alloc_size);
  for (int i = 0; i < 16; i++) {
    functionMemory[16 + i] = 0;
  }

  StatePair regState = {};

  // make memory cap
  char* __capability memory_cap =
      (__cheri_tocap char* __capability)functionMemory;
  memory_cap = __builtin_cheri_bounds_set(memory_cap, alloc_size);
  const unsigned long memory_permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(memory_cap)
                   : "r"(memory_permission_mask));
  // make function cap
  void* __capability function_cap = __builtin_cheri_program_counter_get();
  function_cap =
      __builtin_cheri_address_set(function_cap, (unsigned long)overwriteAll);
  function_cap = __builtin_cheri_bounds_set(function_cap, overwriteSize);
  const unsigned long function_permission_mask = ~(code_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(function_cap)
                   : "r"(function_permission_mask));

  char err = sandboxedCallWrapped(memory_cap, function_cap, 0,
                                  (void*)alloc_size, &regState);
  TEST_ASSERT_EQUAL_INT8_MESSAGE(0, err, "Execute call returned error");
  // check the function actually ran
  int sanityValue = *(int*)(functionMemory + alloc_size - 4);
  TEST_ASSERT_EQUAL_INT32_MESSAGE(
      SANITY_VALUE, sanityValue,
      "Sanity value missmatch, function may not have run");

  free(functionMemory);

  return regState;
}

void testSIGPROThandling(void) {
  int alloc_size = sizeof(void* __capability) * 2;
  char* functionMemory = malloc(alloc_size);
  for (int i = 0; i < sizeof(void* __capability); i++) {
    functionMemory[sizeof(void* __capability) + i] = 0;
  }

  // make memory cap
  char* __capability memory_cap =
      (__cheri_tocap char* __capability)functionMemory;
  memory_cap = __builtin_cheri_bounds_set(memory_cap, alloc_size);
  const unsigned long memory_permission_mask = ~(memory_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(memory_cap)
                   : "r"(memory_permission_mask));
  // make function cap
  void* __capability function_cap = __builtin_cheri_program_counter_get();
  function_cap =
      __builtin_cheri_address_set(function_cap, (unsigned long)triggerSigProt);
  function_cap = __builtin_cheri_bounds_set(function_cap, triggerSigProtSize);
  const unsigned long function_permission_mask = ~(code_permissions);
  __asm__ volatile("clrperm %w0, %w0, %1"
                   : "+r"(function_cap)
                   : "r"(function_permission_mask));

  char err = cheri_execute(memory_cap, function_cap, 0, (void*)alloc_size);
  TEST_ASSERT_EQUAL_INT8_MESSAGE(3, err, "Execute did not");
  // check the function actually ran
  int sanityValue = *(int*)(functionMemory + alloc_size - 4);
  TEST_ASSERT_EQUAL_INT32_MESSAGE(
      SANITY_VALUE, sanityValue,
      "Sanity value missmatch, function may not have run");

  free(functionMemory);
}

void testStackCapabilityRestoration(void) {
  StatePair regState = getSandboxExitState();

  testCapabiltyEquality(regState.expected.csp, regState.actual.csp);
}

void testDDCRestoration(void) {
  StatePair regState = getSandboxExitState();

  testCapabiltyEquality(regState.expected.ddc, regState.actual.ddc);
}

void testCompartmentIdRestoration(void) {
  StatePair regState = getSandboxExitState();

  testCapabiltyEquality(regState.expected.cid, regState.actual.cid);
}

void testThreadIdRestoration(void) {
  StatePair regState = getSandboxExitState();

  testCapabiltyEquality(regState.expected.ctpidr, regState.actual.ctpidr);
}

void testRestrictedThreadIdRestoration(void) {
  StatePair regState = getSandboxExitState();

  testCapabiltyEquality(regState.expected.rctpidr, regState.actual.rctpidr);
}

void testRegisterSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.c0, regState.actual.c0);
  testCapabiltyEquality(regState.expected.c1, regState.actual.c1);
  testCapabiltyEquality(regState.expected.c2, regState.actual.c2);
  testCapabiltyEquality(regState.expected.c3, regState.actual.c3);
  testCapabiltyEquality(regState.expected.c4, regState.actual.c4);
  testCapabiltyEquality(regState.expected.c5, regState.actual.c5);
  testCapabiltyEquality(regState.expected.c6, regState.actual.c6);
  testCapabiltyEquality(regState.expected.c7, regState.actual.c7);
  testCapabiltyEquality(regState.expected.c8, regState.actual.c8);
  testCapabiltyEquality(regState.expected.c9, regState.actual.c9);
  testCapabiltyEquality(regState.expected.c10, regState.actual.c10);
  testCapabiltyEquality(regState.expected.c11, regState.actual.c11);
  testCapabiltyEquality(regState.expected.c12, regState.actual.c12);
  testCapabiltyEquality(regState.expected.c13, regState.actual.c13);
  testCapabiltyEquality(regState.expected.c14, regState.actual.c14);
  testCapabiltyEquality(regState.expected.c15, regState.actual.c15);
  testCapabiltyEquality(regState.expected.c16, regState.actual.c16);
  testCapabiltyEquality(regState.expected.c17, regState.actual.c17);
  testCapabiltyEquality(regState.expected.c18, regState.actual.c18);
  testCapabiltyEquality(regState.expected.c19, regState.actual.c19);
  testCapabiltyEquality(regState.expected.c20, regState.actual.c20);
  testCapabiltyEquality(regState.expected.c21, regState.actual.c21);
  testCapabiltyEquality(regState.expected.c22, regState.actual.c22);
  testCapabiltyEquality(regState.expected.c23, regState.actual.c23);
  testCapabiltyEquality(regState.expected.c24, regState.actual.c24);
  testCapabiltyEquality(regState.expected.c25, regState.actual.c25);
  testCapabiltyEquality(regState.expected.c26, regState.actual.c26);
  testCapabiltyEquality(regState.expected.c27, regState.actual.c27);
  testCapabiltyEquality(regState.expected.c28, regState.actual.c28);
  testCapabiltyEquality(regState.expected.c29, regState.actual.c29);
}

void testStackPointerSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.csp, regState.actual.csp);
}

void testDDCSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.ddc, regState.actual.ddc);
}

void testCompartmentIdSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.cid, regState.actual.cid);
}

void testThreadIdSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.ctpidr, regState.actual.ctpidr);
}

void testRestrictedThreadIdSanitation(void) {
  StatePair regState = getSandboxEntryState();

  testCapabiltyEquality(regState.expected.rctpidr, regState.actual.rctpidr);
}

int main(int argc, char const* argv[]) {
  UNITY_BEGIN();
  // test register restoring
  RUN_TEST(testStackCapabilityRestoration);
  RUN_TEST(testDDCRestoration);
  RUN_TEST(testCompartmentIdRestoration);
  RUN_TEST(testThreadIdRestoration);
  RUN_TEST(testRestrictedThreadIdRestoration);
  // check registers after function entry
  RUN_TEST(testRegisterSanitation);
  RUN_TEST(testStackPointerSanitation);
  RUN_TEST(testDDCSanitation);
  RUN_TEST(testCompartmentIdSanitation);
  RUN_TEST(testThreadIdSanitation);
  RUN_TEST(testRestrictedThreadIdSanitation);
  // exception handling tests
  // RUN_TEST(testSIGPROThandling);
  return UNITY_END();
}
