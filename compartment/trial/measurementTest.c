#include <stdint.h>
#include <stdio.h>

int main(int argc, char const *argv[]) {
  #define ADDITION "add %[test], %[test], #1 \n"
  #define DO2(MAKRO) MAKRO MAKRO
  #define DO4(MAKRO) DO2(DO2(MAKRO))
  #define DO16(MAKRO) DO4(DO4(MAKRO))
  uint64_t start, end, test;
  test=0;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(start));
  for(int i = 0; i < 8096; i++){
  __asm__ __volatile__(
    DO4(DO16(DO16(ADDITION)))
     : [test]"+r"(test)
   );
  }
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(end));

  printf("start %lu\n", start);
  printf("end %lu\n", end);
  printf("time %lu\n", end-start);

  printf("test_move is now: %lu \n", test);
  return 0;
}
