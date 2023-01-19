// include for printing
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/module.h>
#include <sys/mman.h>
#include <sys/cpuset.h>
// include to use cheribsd dynamic loader
#include <fcntl.h>
#include <elf.h>
#include <errno.h>
#include <unistd.h>
// internal includes
#include "compartment.h"
#include "elfParsing.h"
#include "dandelionIOInternal.h"
#include "functionManagement.h"

int main(int argc, char const *argv[]) {
  printf("Server Hello\n");

  // restrict server process to run on only one cpu for now
  // to make sure we are running on the cpu where the syscall set up the registers
  cpuset_t cpuMask;
  cpuset_getaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, -1, sizeof(cpuMask), &cpuMask);
  printf("Can access %d CPUs\n", CPU_COUNT(&cpuMask));
  cpuset_t cpuNew;
  CPU_SETOF(CPU_FFS(&cpuMask)-1, &cpuNew);
  printf("Can access %d CPUs\n", CPU_COUNT(&cpuNew));
  cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, -1, sizeof(cpuNew), &cpuNew);
  int syscallId = modfind("sys/dandelionCPUSettings");
  if(syscallId == -1){
    printf("could not find dandelionCPUSettings\n");
    return -1;
  }
  struct module_stat stat;
  stat.version = sizeof(stat);
  int syserr =  modstat(syscallId, &stat);
  if(syserr != 0){
    printf("could not stat id\n");
    return -1;
  }
  syscall(stat.data.intval);

  // open file
  int elf_file = open("wrapper", O_RDONLY);

  if(addFunctionFromStaticElf(1, elf_file, 1<<26, 1) != 0){
    printf("Error when adding function\n");
  }

  // check if the descriptor has what would be expected
  int inputInt = 11;
  ioStruct inputStruct = {.size = 4, .address = &inputInt};
  ioStruct* outputStruct;
  int outputCount;
   runFunction(1, &inputStruct, 1, &outputStruct, &outputCount);
   printf("output count %d\n", outputCount);
   printf("output %d\n", *((int*)outputStruct->address));
   printf("Server Goodbye\n");

   return 0;
}
