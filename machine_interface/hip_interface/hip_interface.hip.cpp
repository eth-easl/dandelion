#include <hip/hip_runtime.h>
#include <stdint.h>

__global__ void toy_kernel() {
    printf("Kernel launched!\n");
}

extern "C" void gpu_toy_launch(uint8_t gpu_id) {
    hipSetDevice(gpu_id);

    toy_kernel<<<dim3(1), dim3(1), 0, hipStreamDefault>>>();

    hipDeviceSynchronize();
}