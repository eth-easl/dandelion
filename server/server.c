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
  // read the program section header table from the file
  Elf_Off shtOffset = header.e_shoff;
  Elf_Half shtEntryQuantity = header.e_shnum;
  Elf_Half shtEntrySize = header.e_shentsize;
  long int shtSize = shtEntryQuantity*shtEntrySize;
  Elf_Shdr* shTable = malloc(shtSize);
  if(pread(elf_file, shTable, shtSize, shtOffset) != shtSize){
    perror(strerror(errno));
    return -1;
  }

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
      printf("Loading Address %lu\n", virtualAddress);
      printf("Loading Size    %lu\n", loadSize);
      pread(elf_file, functionMemoryAddress+virtualAddress, loadSize, fileOffset);
    }
  }

  // make ppc
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionMemoryAddress);
  pcc = __builtin_cheri_bounds_set(pcc, memorySize);
  pcc = pcc + entryPoint;

  // load symbol table
  int symTabIndex = 0;
  while(symTabIndex < shtEntryQuantity && shTable[symTabIndex].sh_type != SHT_SYMTAB){
    symTabIndex++;
  }
  if(symTabIndex == shtEntryQuantity){
    printf("No symtab found\n");
    return -1;
  }

  Elf_Off symTabOffset = shTable[symTabIndex].sh_offset;
  Elf_Word symTabSize = shTable[symTabIndex].sh_size;
  Elf_Word symTabEntrySize = shTable[symTabIndex].sh_entsize;
  int symTabEntryQuantity = symTabSize/symTabEntrySize;

   Elf_Sym* symTable = malloc(symTabSize);
   if(pread(elf_file, symTable, symTabSize, symTabOffset) != symTabSize){
     perror(strerror(errno));
     return -1;
   }

   // load section name string table
   Elf_Half sectionNameTableIndex = header.e_shstrndx;
   Elf_Off sectionNameTableOffset = shTable[sectionNameTableIndex].sh_offset;
   Elf_Word sectionNameTableSize = shTable[sectionNameTableIndex].sh_size;
   char* sectionNameTable = malloc(sectionNameTableSize);
   if(pread(elf_file, sectionNameTable, sectionNameTableSize, sectionNameTableOffset) != sectionNameTableSize){
     perror(strerror(errno));
     return -1;
   }

   printf("index %d\n", symTabIndex);
   printf("index %d\n", shTable[symTabIndex].sh_name);
   printf("index %s\n", sectionNameTable+shTable[symTabIndex].sh_name);

   // load symbol name table
   int symbolStringTableIndex = 0;
   while(symbolStringTableIndex < shtEntryQuantity){
     int comparison = strcmp(".strtab",
      sectionNameTable + shTable[symbolStringTableIndex].sh_name);
     if(comparison == 0){break;}
     symbolStringTableIndex++;
   }

   Elf_Word symStringTabSize = shTable[symbolStringTableIndex].sh_size;
   Elf_Word symStringTabOffset = shTable[symbolStringTableIndex].sh_offset;
   char* symbolStringTable = malloc(symStringTabSize);
   if(pread(elf_file, symbolStringTable, symStringTabSize, symStringTabOffset) != symStringTabSize){
     perror(strerror(errno));
     return -1;
   }

   printf("symbolStringTable %s\n", sectionNameTable+shTable[symbolStringTableIndex].sh_name);

   for(int i = symTabEntryQuantity-2; i<symTabEntryQuantity; i++){
     printf("Symbol: %s\n", symbolStringTable+symTable[i].st_name);
     printf("value: %lu\n", symTable[i].st_value);
     printf("size: %lu\n", symTable[i].st_size);
     printf("mem value %d\n", *((char*)functionMemoryAddress + symTable[i].st_value));
   }

  sandboxedCall(pcc, functionMemory, (void*) memorySize -16);

   printf("symbolStringTable %s\n", sectionNameTable+shTable[symbolStringTableIndex].sh_name);

   for(int i = symTabEntryQuantity-2; i<symTabEntryQuantity; i++){
     printf("Symbol: %s\n", symbolStringTable+symTable[i].st_name);
     printf("value: %lu\n", symTable[i].st_value);
     printf("size: %lu\n", symTable[i].st_size);
     printf("mem value %d\n", *((char*)functionMemoryAddress + symTable[i].st_value));
   }


  printf("Server Goodbye\n");

  return 0;
}
