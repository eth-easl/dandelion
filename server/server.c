// include for printing
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/module.h>
// include to use cheribsd dynamic loader
#include <fcntl.h>
#include <elf.h>
#include <errno.h>
#include <unistd.h>

int main(int argc, char const *argv[]) {
  printf("Server Hello\n");

  int syscallId = modfind("sys/dandelionCPUSettings");
  if(syscallId == -1){
    printf("could not find dandelionCPUSettings\n");
    return 0;
  }
  struct module_stat stat;
  stat.version = sizeof(stat);
  int syserr =  modstat(syscallId, &stat);
  if(syserr != 0){
    printf("could not stat id\n");
    return 0;
  }
  syscall(stat.data.intval);

  // save ddc
  void* __capability ddc;
  __asm__ volatile("mrs %x0, DDC" : "=r"(ddc));

  int arrayTest[] = {1,2,3,4,5,6,7,8,9};
  int* __capability arrayPointer = arrayTest;
  int testValue = 0;
  __asm__ volatile(
    "msr DDC, %x1 \n"
    "mov x0, #0 \n"
    "ldr %w0, [x0] \n"
    "msr DDC, %x2 \n"
    : "+r"(testValue) : "r"(arrayPointer), "r"(ddc) : "r0"
  );
  printf("testValue: %d\n", testValue);
  // // open file
  // int elf_file = open("arm.out", O_RDONLY);
  //
  // // read the elf Header
  // Elf_Ehdr header = {};
  // if(read(elf_file, &header, sizeof(Elf_Ehdr)) != sizeof(Elf_Ehdr)){
  //   perror(strerror(errno));
  // }
  //
  // // get the program header table (array of progam headers)
  // Elf_Off phOff = header.e_phoff;
  // printf("Program Header Table Offset %lu\n", phOff);
  // Elf_Half phNum = header.e_phnum; // number of entries in the progam header table
  // printf("Program Header Table Entries %d\n", phNum);
  //
  // Elf_Phdr* phTable = malloc(sizeof(Elf_Phdr)*phNum);
  // if(pread(elf_file, phTable, sizeof(Elf_Phdr)*phNum, phOff) != sizeof(Elf_Phdr)*phNum){
  //   perror(strerror(errno));
  // }
  //
  // printf("first Header type %d\n", phTable[0].p_type);
  // printf("PT_PHDR %d\n", PT_PHDR);

  printf("server finished\n");

  return 0;
}
