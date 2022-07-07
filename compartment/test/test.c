// System Headers

// Standard Libraries

// Project External Libraries
#include "unity.h"
// Project Internal Libraries

void setUp(void){}
void tearDown(void){}

void testRegisterSanitation(void){
  TEST_ASSERT_TRUE(1);
}

int main(int argc, char const *argv[]) {
  UNITY_BEGIN();
  RUN_TEST(testRegisterSanitation);
  return UNITY_END();
}
