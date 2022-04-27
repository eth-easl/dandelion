// include for printing
#include <stdio.h>

int main(int argc, char const *argv[]) {

  int test_array[8] = {0,1,2,3,4,5,6,7};
  int test_load = -1;
  int test_move = -1;

  // turn off capability mode
  __asm__ volatile(
    "bx #4\n" // turn off capability mode
    "mov %w[input], 9\n"
    "ldr %w[load_target] ,[%w[pointer]]\n"
    "bx #4\n" // turn on capability mode
    : [input] "=r" (test_move),
    [load_target] "=r" (test_load)
    : [pointer] "r" (test_array)
  );
  // turn capability mode back on
  printf("test_move is now: %d\n", test_move);
  printf("test_move is now: %d\n", test_load);
  return 0;
}
