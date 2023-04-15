// System Headers
#include <stdlib.h>
#include <string.h>
// Standard Libraries

// Project External Libraries
#include "unity.h"
// Project Internal Libraries
#include "memory.h"

// check the bounds are as expected
void bounds_and_permission_test(size_t size) {
  cheri_context* test_context = cheri_alloc(size);
  if (test_context == NULL) {
    return;
  }
  // test the size
  TEST_ASSERT_GREATER_OR_EQUAL_INT64_MESSAGE(size, test_context->size,
                                             "mapped size test");
  // test the capability
  size_t base = __builtin_cheri_base_get(test_context->cap);
  size_t address = __builtin_cheri_address_get(test_context->cap);
  // automatically also checks if the allignment is correctly chosen (a.k.a.
  // representable)
  TEST_ASSERT_EQUAL_INT64_MESSAGE(address, base, "address to base comparison");
  size_t rounded_size = sandbox_size_rounding(size);
  size_t cap_size = __builtin_cheri_length_get(test_context->cap);
  TEST_ASSERT_GREATER_OR_EQUAL_INT64_MESSAGE(size, cap_size,
                                             "capability minimal size");
  TEST_ASSERT_LESS_OR_EQUAL_INT64_MESSAGE(test_context->size, cap_size,
                                          "capability maximal size");
  size_t tag = __builtin_cheri_tag_get(test_context->cap);
  TEST_ASSERT_TRUE_MESSAGE(tag, "Tag check failed");
  size_t perms = __builtin_cheri_perms_get(test_context->cap);
  TEST_ASSERT_EQUAL_HEX64_MESSAGE(memory_permissions, perms,
                                  "Permission missmatch");
}

// check that allocations have proper capabilities set up
void allocation_test(void) {
  // iterate through powers of two and their bordering values
  for (int i = 1; i < 63; i++) {
    size_t value = (1L << i);
    bounds_and_permission_test(value);
    bounds_and_permission_test(value + 1);
    bounds_and_permission_test(value - 1);
  }
}

void overallocation_test(void) {
  cheri_context* test_context = cheri_alloc(-1LL);
  TEST_ASSERT_EQUAL_HEX64(NULL, test_context);
  test_context = cheri_alloc(18446744073709551615);
  TEST_ASSERT_EQUAL_HEX64(NULL, test_context);
}

// functions to call before and after each test
void setUp(void) {}
void tearDown(void) {}

int main(int argc, char const* argv[]) {
  UNITY_BEGIN();
  RUN_TEST(allocation_test);
  RUN_TEST(overallocation_test);
  return UNITY_END();
}