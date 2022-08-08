// System Headers
#include <stdlib.h>
#include <string.h>
// Standard Libraries

// Project External Libraries
#include "unity.h"
// Project Internal Libraries
#include "elfParsing.h"

void setUp(void){}
void tearDown(void){}

// todo implement testing for elf file parsing
void testElfPopulate(void){
  TEST_ASSERT_TRUE_MESSAGE( 0, "Not yet implemented" );
};

void testGetSymbolAddress(void){
  TEST_ASSERT_TRUE_MESSAGE( 0, "Not yet implemented" );
}

int main(int argc, char const *argv[]) {
  UNITY_BEGIN();
  RUN_TEST(testElfPopulate);
  RUN_TEST(testGetSymbolAddress);
  return UNITY_END();
}
