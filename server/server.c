// include for printing
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/module.h>
#include <sys/mman.h>
// include to use cheribsd dynamic loader
#include <fcntl.h>
#include <elf.h>
#include <errno.h>
#include <unistd.h>
// internal includes
#include "compartment.h"
#include "elfParsing.h"

int main(int argc, char const *argv[]) {
  printf("Server Hello\n");

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

  elfDescriptor elf = {};
  if(populateElfDescriptor(elf_file, &elf) != 0){return -1;}

  // allocate space for the process to execute in
  size_t memorySize = 1L<<32;
  void* functionMemoryAddress = mmap(NULL,
    memorySize,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );
  void*__capability functionMemory =
    (__cheri_tocap void*__capability) functionMemoryAddress;
  functionMemory = __builtin_cheri_bounds_set(functionMemory, memorySize);

  // process loadable headers
  for (size_t headerIndex = 0; headerIndex < elf.elfHeader.e_shnum; headerIndex++) {
    if(elf.programHeaderTable[headerIndex].p_type == PT_LOAD){
      Elf_Phdr loadedHeader = elf.programHeaderTable[headerIndex];
      Elf_Off fileOffset = loadedHeader.p_offset;
      Elf_Addr virtualAddress = loadedHeader.p_vaddr;
      Elf_Word loadSize = loadedHeader.p_filesz;
      printf("Loading Address %lu\n", virtualAddress);
      printf("Loading Size    %u\n", loadSize);
      pread(elf_file, functionMemoryAddress+virtualAddress, loadSize, fileOffset);
    }
  }

  // make ppc
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionMemoryAddress);
  pcc = __builtin_cheri_bounds_set(pcc, memorySize);
  pcc = pcc + elf.elfHeader.e_entry;

  Elf_Addr testAddr = 0;
  Elf_Word testSize = 0;
  getSymbolAddress(&elf, "test_initialized", &testAddr, &testSize);
  printf("test_initialized address %lu\n", testAddr);
  printf("test_initialized size %u\n", testSize);
  printf("test_initialized value before %d\n", *(int*)(functionMemoryAddress+testAddr));

   sandboxedCall(pcc, functionMemory, (void*) memorySize -16);

   printf("test_initialized value after %d\n", *(int*)(functionMemoryAddress+testAddr));

   freeElfDescriptor(&elf);

   printf("Server Goodbye\n");

   return 0;
}
