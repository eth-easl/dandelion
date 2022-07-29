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

  // read the elf Header
  Elf_Ehdr header = {};
  if(read(elf_file, &header, sizeof(Elf_Ehdr)) != sizeof(Elf_Ehdr)){
    perror(strerror(errno));
    return -1;
  }
  Elf_Off entryPoint = header.e_entry;

  // offset in the file for the the program header table
  Elf_Off phtOffset = header.e_phoff;
  // number of entries in the progam header table
  Elf_Half phtEntryQuantity = header.e_phnum;
  // size of each entry in the program header table
  Elf_Half phtEntrySize = header.e_phentsize;
  long int phtSize = phtEntryQuantity*phtEntrySize;
  // read the program header table from the file
  Elf_Phdr* phTable = malloc(phtSize);
  if(pread(elf_file, phTable, phtSize, phtOffset) != phtSize){
    perror(strerror(errno));
    return -1;
  }

  // allocate space for the process to execute in
  size_t memorySize = 1L<<32;
  void*__capability functionMemory = mmap(NULL,
    memorySize,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );

  printf("functionMemory flags: %ld\n", __builtin_cheri_perms_get(functionMemory));
  printf("functionMemory addr: %ld\n", __builtin_cheri_address_get(functionMemory));
  printf("functionMemory base: %ld\n", __builtin_cheri_base_get(functionMemory));
  printf("functionMemory length: %ld\n", __builtin_cheri_length_get(functionMemory));

  // printf("PT_PHDR %d\n", PT_PHDR);
  // printf("PT_LOAD %d\n", PT_LOAD);
  for (size_t headerIndex = 0; headerIndex < phtEntryQuantity; headerIndex++) {
    // process loadable headers
    // printf("first Header type %d\n", phTable[headerIndex].p_type);
    if(phTable[headerIndex].p_type == PT_LOAD){
      Elf_Phdr loadedHeader = phTable[headerIndex];
      Elf_Off fileOffset = loadedHeader.p_offset;
      Elf_Addr virtualAddress = loadedHeader.p_vaddr;
      Elf_Word loadSize = loadedHeader.p_filesz;
      pread(elf_file, functionMemory+virtualAddress, loadSize, fileOffset);
    }
  }

  // make ppc
  void* __capability pcc = functionMemory + entryPoint;
  printf("functionMemory flags: %ld\n", __builtin_cheri_perms_get(pcc));
  printf("functionMemory addr: %ld\n", __builtin_cheri_address_get(pcc));
  printf("functionMemory base: %ld\n", __builtin_cheri_base_get(pcc));
  printf("functionMemory length: %ld\n", __builtin_cheri_length_get(pcc));
  __asm__ volatile(
    "msr ddc, %x0 \n"
    "mov sp, %1 \n"
    "blr %x2"
    : : "r"(functionMemory), "r"(memorySize), "r"(pcc));

  printf("Server Goodbye\n");

  return 0;
}
