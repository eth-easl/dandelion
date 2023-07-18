# /bin/sh

cpuset -l 0 kldload ./dandelionKernelModule.ko
cpuset -l 0 kldunload ./dandelionKernelModule.ko
cpuset -l 1 kldload ./dandelionKernelModule.ko
cpuset -l 1 kldunload ./dandelionKernelModule.ko
cpuset -l 2 kldload ./dandelionKernelModule.ko
cpuset -l 2 kldunload ./dandelionKernelModule.ko
cpuset -l 3 kldload ./dandelionKernelModule.ko
cpuset -l 3 kldunload ./dandelionKernelModule.ko
