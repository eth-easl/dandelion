#include "dandelion/crt.h"
#include "dandelion/runtime.h"

static char output_buffer[] = "test string 123";

int main() {
  IoBuffer output = {
    .ident = "result",
    .ident_len = 6,
    .data = output_buffer,
    .data_len = sizeof(output_buffer) - 1,
    .key = 0
  };
  dandelion_add_output(0, output);
  return 0;
}

DANDELION_ENTRY(main)
