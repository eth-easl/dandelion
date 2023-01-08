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
//
#include "compartment.h"

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

  Elf_Addr max_code_addr = 0;

  // unused for now
  Elf_Addr max_data_addr = 0;

  for (size_t headerIndex = 0; headerIndex < phtEntryQuantity; headerIndex++) {
    Elf_Phdr header = phTable[headerIndex];
    if(header.p_type == PT_LOAD){
      Elf_Addr segment_addr_bound = header.p_vaddr + header.p_memsz;

      Elf_Word flags = header.p_flags;
      if (flags & PF_X) {
        if (segment_addr_bound > max_code_addr) {
          max_code_addr = segment_addr_bound;
        }
      } else {
        if (segment_addr_bound > max_data_addr) {
          max_data_addr = segment_addr_bound;
        }
      }
    }
  }

  // by default, choose a large memory size so that we can fit heap and stack
  size_t memorySize = 1L<<32;

  void* code_ptr = mmap(NULL,
    max_code_addr,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );

  void* data_ptr = mmap(NULL,
    memorySize,
    PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );

  for (size_t headerIndex = 0; headerIndex < phtEntryQuantity; headerIndex++) {
    // process loadable headers
    // printf("first Header type %d\n", phTable[headerIndex].p_type);
    Elf_Phdr header = phTable[headerIndex];
    if(header.p_type == PT_LOAD){
      Elf_Off fileOffset = header.p_offset;
      Elf_Addr virtualAddress = header.p_vaddr;
      Elf_Word fileSize = header.p_filesz;

      Elf_Word flags = header.p_flags;
      // if the X (exec) flag is set, map into code section, else map as data
      void* base;
      if (flags & PF_X) {
        base = code_ptr;
      } else {
        base = data_ptr;
      }
      pread(elf_file, base + virtualAddress, fileSize, fileOffset);
    }
  }

  // make ppc
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)code_ptr);
  pcc = __builtin_cheri_bounds_set(pcc, max_code_addr);
  pcc = pcc + entryPoint;

  void*__capability ddc =
    (__cheri_tocap void*__capability) data_ptr;
  ddc = __builtin_cheri_bounds_set(ddc, memorySize);

  printf("pcc perms : %ld\n", __builtin_cheri_perms_get(pcc));
  printf("pcc addr  : %ld\n", __builtin_cheri_address_get(pcc));
  printf("pcc base  : %ld\n", __builtin_cheri_base_get(pcc));
  printf("pcc length: %ld\n", __builtin_cheri_length_get(pcc));

  printf("ddc perms : %ld\n", __builtin_cheri_perms_get(ddc));
  printf("ddc addr  : %ld\n", __builtin_cheri_address_get(ddc));
  printf("ddc base  : %ld\n", __builtin_cheri_base_get(ddc));
  printf("ddc length: %ld\n", __builtin_cheri_length_get(ddc));

  sandboxedCall(pcc, ddc, (void*) memorySize - 16);

  printf("Server Goodbye\n");

  return 0;
}
