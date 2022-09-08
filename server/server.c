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
#include "dandelionIOInternal.h"

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
    size_t memorySize = 1L << 26;
    size_t mask, rounded;
    __asm__ volatile("RRMASK %0, %1" : "+r"(mask) : "r"(memorySize));
    __asm__ volatile("RRLEN %0, %1" : "+r"(rounded) : "r"(memorySize));
    printf("value %zx\n", memorySize);
    printf("rounded %zx\n", rounded);
    printf("mask %zx\n", mask);
    // get position of first 0
    int allignment = 63;
    while(allignment > 0){
      if(!((1L << allignment) & mask)){
        break;
      }
      allignment--;
    }
    printf("pos %d\n", allignment);
    // gives the left shift to first non 0, this means last 1 is +2
    // 1 for correcting that we detect the first 0
    // and 1 for accounting the original 1
    allignment = allignment + 2;
    memorySize = rounded;

  void* functionMemoryAddress = mmap(NULL,
    memorySize,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS | MAP_ALIGNED(allignment), // get anonymous, page alligned memory
    -1,
    0
  );
  printf("functionMemoryAddress %p\n", functionMemoryAddress);
  if(functionMemoryAddress == MAP_FAILED){
    perror(strerror(errno));
    return -1;
  }

  void*__capability functionMemory =
    (__cheri_tocap void*__capability) functionMemoryAddress;
  printf("function memory base %p\n", (void*)__builtin_cheri_base_get(functionMemory));
  printf("function memory length %lu\n", __builtin_cheri_length_get(functionMemory));
  printf("function memory address %p\n", (void*)__builtin_cheri_address_get(functionMemory));
  functionMemory = __builtin_cheri_bounds_set(functionMemory, memorySize);
  // TODO fix the bounds
  printf("function memory base %p\n", (void*)__builtin_cheri_base_get(functionMemory));
  printf("function memory length %lu\n", __builtin_cheri_length_get(functionMemory));
  printf("function memory address %p\n", (void*)__builtin_cheri_address_get(functionMemory));

  // holds the uppermost virtual address that is already used
  // this will be the bottom address for the heap
  Elf_Addr topVirtualAddress = 0;

  // process loadable headers
  for (size_t headerIndex = 0; headerIndex < elf.elfHeader.e_shnum; headerIndex++) {
    if(elf.programHeaderTable[headerIndex].p_type == PT_LOAD){
      Elf_Phdr loadedHeader = elf.programHeaderTable[headerIndex];
      Elf_Off fileOffset = loadedHeader.p_offset;
      Elf_Addr virtualAddress = loadedHeader.p_vaddr;
      Elf_Word loadSize = loadedHeader.p_filesz;
      Elf_Word virtSize = loadedHeader.p_memsz;
      if(virtSize + virtualAddress > topVirtualAddress){
        topVirtualAddress = virtSize + virtualAddress;
      }
      // printf("Loading Address %lu\n", virtualAddress);
      // printf("Loading Size    %u\n", virtSize);
      pread(elf_file, functionMemoryAddress+virtualAddress, loadSize, fileOffset);
    }
  }

  // make ppc
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionMemoryAddress);
  pcc = __builtin_cheri_bounds_set(pcc, memorySize);
  pcc = pcc + elf.elfHeader.e_entry;

  printf("pcc base %p\n", (void*)__builtin_cheri_base_get(pcc));
  printf("pcc length %lu\n", __builtin_cheri_length_get(pcc));
  printf("pcc address %p\n", (void*)__builtin_cheri_address_get(pcc));

  // set up dandelionIO
  Elf_Addr inputRootAddress = 0;
  Elf_Word inputRootSize = 0;
  getSymbolAddress(&elf, "inputRoot", &inputRootAddress, &inputRootSize);
  Elf_Addr outputRootAddress = 0;
  Elf_Word outputRootSize = 0;
  getSymbolAddress(&elf, "outputRoot", &outputRootAddress, &outputRootSize);
  Elf_Addr inputNumberAddress = 0;
  Elf_Word inputNumberSize = 0;
  getSymbolAddress(&elf, "inputNumber", &inputNumberAddress, &inputNumberSize);
  Elf_Addr outputNumberAddress = 0;
  Elf_Word outputNumberSize = 0;
  getSymbolAddress(&elf, "outputNumber", &outputNumberAddress, &outputNumberSize);
  Elf_Addr maxOutputNumberAddress = 0;
  Elf_Word maxOutputNumberSize = 0;
  getSymbolAddress(&elf, "maxOutputNumber", &maxOutputNumberAddress, &maxOutputNumberSize);
  Elf_Addr returnPairAddress = 0;
  Elf_Word returnPairSize = 0;
  getSymbolAddress(&elf, "returnPair", &returnPairAddress, &returnPairSize);

  // set up input root and structs for input
  int inputNumber = 1;
  *((int*)(functionMemoryAddress+inputNumberAddress)) = inputNumber;
  ioStruct** inputRootPointer = (ioStruct**)(functionMemoryAddress+inputRootAddress);
  *inputRootPointer = (ioStruct*) topVirtualAddress;
  topVirtualAddress += sizeof(ioStruct)*inputNumber;
  // printf("%lu\n", topVirtualAddress);

  int maxOutputNumber = 1;
  *((int*)(functionMemoryAddress+maxOutputNumberAddress)) = maxOutputNumber;
  ioStruct** outputRootPointer = (ioStruct**)(functionMemoryAddress+outputRootAddress);
  *outputRootPointer = (ioStruct*) topVirtualAddress;
  topVirtualAddress += sizeof(ioStruct)*maxOutputNumber;

  // put input on the heap
  int input = 8;
  ((ioStruct*) (functionMemoryAddress+(size_t)*inputRootPointer))->size = 4;
  ((ioStruct*) (functionMemoryAddress+(size_t)*inputRootPointer))->address = (void*) topVirtualAddress;
  *((int*)(functionMemoryAddress+topVirtualAddress)) = input;
  topVirtualAddress += sizeof(int);
  printf("input number %d\n", inputNumber);
  printf("input address %p\n", ((ioStruct*) (functionMemoryAddress+(size_t)*inputRootPointer))->address);
  printf("input value %d\n", input);

  sandboxedCall(pcc, functionMemory, returnPairAddress, (void*) memorySize);

   // check output
   int output = *((int*) (functionMemoryAddress + (size_t)outputAddress));
   printf("output value %d\n", output);

   freeElfDescriptor(&elf);

   printf("Server Goodbye\n");

   return 0;
}
