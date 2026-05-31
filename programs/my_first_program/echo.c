#include "dandelion/crt.h"
#include "dandelion/runtime.h"

static char output_buffer[1024];

int main() {
  IoBuffer *input = dandelion_get_input(0, 0);

  size_t copy_len = input->data_len < 1024 ? input->data_len : 1024;
  for (size_t i = 0; i < copy_len; i++) {
    output_buffer[i] = ((char *)input->data)[i];
  }

  IoBuffer output = {"output", 6, output_buffer, copy_len};
  dandelion_add_output(0, output);

  return 0;
}

DANDELION_ENTRY(main)
