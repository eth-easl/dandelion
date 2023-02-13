
// Implementing headers
#include "elfParsing.h"
// System Headers
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries

// TODO: for all perrors of subfunctions introduce special case for reading end of file.
// Special case is needed, as reading eof will not set errno and have empty string for error.
// This means that no error will be logged.
int populateElfDescriptor(int file, elfDescriptor* descriptor){
  int returnValue = populateElfHeader(file,descriptor);
  if(returnValue != 0){return returnValue;}

  returnValue = populateProgramHeaderTable(file,descriptor);
  if(returnValue != 0){freeElfDescriptor(descriptor); return returnValue;}

  returnValue = populateSectionHeaderTable(file,descriptor);
  if(returnValue != 0){freeElfDescriptor(descriptor); return returnValue;}

  returnValue = populateSectionNameTable(file,descriptor);
  if(returnValue != 0){freeElfDescriptor(descriptor); return returnValue;}

  if((returnValue = populateSymbolTable(file,descriptor)) != 0){
    freeElfDescriptor(descriptor);
    return returnValue;
  }

  if((returnValue = populateSymbolNameTable(file,descriptor)) != 0){
    freeElfDescriptor(descriptor);
    return returnValue;
  }

  return 0;
}

void freeElfDescriptor(elfDescriptor* desc){
  if(desc->programHeaderTable != NULL) {free(desc->programHeaderTable);}
  if(desc->sectionHeaderTable != NULL) {free(desc->sectionHeaderTable);}
  if(desc->sectionNameTable != NULL) {free(desc->sectionNameTable);}
  if(desc->symbolTable != NULL) {free(desc->symbolTable);}
  if(desc->symbolNameTable != NULL) {free(desc->symbolNameTable);}
}

int populateElfHeader(int elfFile, elfDescriptor* elfDescriptor){
  // read the header from the file
  int headerSize = sizeof(Elf_Ehdr);
  if(read(elfFile, &(elfDescriptor->elfHeader), headerSize) != headerSize){
    perror(strerror(errno));
    return -1;
  }
  return 0;
}

int populateProgramHeaderTable(int elfFile, elfDescriptor* elfDescriptor){
  // file offset for program header table
  Elf_Off phtOffset = elfDescriptor->elfHeader.e_phoff;
  // number of entries in the progam header table
  Elf_Half phtEntryQuantity = elfDescriptor->elfHeader.e_phnum;
  // size of each entry in the program header table
  Elf_Half phtEntrySize = elfDescriptor->elfHeader.e_phentsize;
  // size of program header table in bytes
  long int phtSize = phtEntryQuantity*phtEntrySize;
  // allocate memory for the program header table
  Elf_Phdr* programHeaderTable = malloc(phtSize);
  if (programHeaderTable == NULL){ return -1;}
  // read from file
  if(pread(elfFile, programHeaderTable, phtSize, phtOffset) != phtSize){
    perror(strerror(errno));
    return -1;
  }
  elfDescriptor->programHeaderTable = programHeaderTable;
  return 0;
}

int populateSectionHeaderTable(int elfFile, elfDescriptor* elfDescriptor){
  // file offset for the section header table
  Elf_Off shtOffset = elfDescriptor->elfHeader.e_shoff;
  // number of entries in the section header table
  Elf_Half shtEntryQuantity = elfDescriptor->elfHeader.e_shnum;
  // size of each entry in the section header table
  Elf_Half shtEntrySize = elfDescriptor->elfHeader.e_shentsize;
  // size of the section header table in bytes
  long int shtSize = shtEntryQuantity*shtEntrySize;
  // allocate memory for section header table
  Elf_Shdr* sectionHeaderTable = malloc(shtSize);
  if(sectionHeaderTable == NULL){ return -1;}
  // read from file
  if(pread(elfFile, sectionHeaderTable, shtSize, shtOffset) != shtSize){
    perror(strerror(errno));
    return -1;
  }
  elfDescriptor->sectionHeaderTable = sectionHeaderTable;
  return 0;
}

int populateSectionNameTable(int elfFile, elfDescriptor* elfDescriptor){
  // get section index with section name table
  Elf_Half sectionNameTableIndex = elfDescriptor->elfHeader.e_shstrndx;
  // get section file offset
  Elf_Off sectionNameTableOffset =
    elfDescriptor->sectionHeaderTable[sectionNameTableIndex].sh_offset;
  // get section size
  Elf_Word sectionNameTableSize =
    elfDescriptor->sectionHeaderTable[sectionNameTableIndex].sh_size;
  // allocate space
  char* sectionNameTable = malloc(sectionNameTableSize);
  if(sectionNameTable == NULL){return -1;}
  // read from file
  if(pread(elfFile, sectionNameTable, sectionNameTableSize,
    sectionNameTableOffset) != sectionNameTableSize){
      perror(strerror(errno));
      return -1;
  }
  elfDescriptor->sectionNameTable = sectionNameTable;
  return 0;
}

int populateSymbolTable(int elfFile, elfDescriptor* elfDescriptor){
  // find section index for section containing symbol table
  int stIndex = 0;
  int entries = elfDescriptor->elfHeader.e_shnum;
  Elf_Shdr* sectionHeaderTable = elfDescriptor->sectionHeaderTable;
  for(; stIndex < entries; stIndex++){
    if(sectionHeaderTable[stIndex].sh_type == SHT_SYMTAB){
      break;
    }
  }
  // check if symbol table was found
  if(stIndex == entries){return -1;}
  // symbol table offset in file
  Elf_Off stOffset = sectionHeaderTable[stIndex].sh_offset;
  // size of the symbol table
  Elf_Word stSize = sectionHeaderTable[stIndex].sh_size;
  // allocate memory
  Elf_Sym* symbolTable = malloc(stSize);
  if(symbolTable == NULL){return -1;}
  // read from file
  if(pread(elfFile, symbolTable, stSize, stOffset) != stSize){
    perror(strerror(errno));
    return -1;
  }
  elfDescriptor->symbolTable = symbolTable;
  elfDescriptor->symbolTableEntries =
    stSize / sectionHeaderTable[stIndex].sh_entsize;
  return 0;
}

int populateSymbolNameTable(int elfFile, elfDescriptor* elfDescriptor){
  // find section containing symbol string table
  int sntIndex = 0;
  int entries = elfDescriptor->elfHeader.e_shnum;
  Elf_Shdr* sectionHeaderTable = elfDescriptor->sectionHeaderTable;
  char* sectionNameTable = elfDescriptor->sectionNameTable;
  for(; sntIndex < entries; sntIndex++){
    int comparison =
      strcmp(".strtab",sectionNameTable+sectionHeaderTable[sntIndex].sh_name);
    if(comparison == 0){break;}
  }
  // check if it was found
  if(sntIndex == entries){return -1;}
  // get offset in file
  Elf_Off sntOffset = sectionHeaderTable[sntIndex].sh_offset;
  // get size of symbol name table
  Elf_Word sntSize = sectionHeaderTable[sntIndex].sh_size;
  // allocate memory
  char* symbolNameTable = malloc(sntSize);
  if(symbolNameTable == NULL){return -1;}
  // read from file
  if(pread(elfFile, symbolNameTable, sntSize, sntOffset) != sntSize){
    perror(strerror(errno));
    return -1;
  }
  elfDescriptor->symbolNameTable = symbolNameTable;
  return 0;
}

int getSymbolAddress(elfDescriptor* elfDescriptor, const char* symbolName,
  Elf_Addr* address, Elf_Word* size){
    Elf_Sym* symbolTable = elfDescriptor->symbolTable;
    char* symbolNameTable = elfDescriptor->symbolNameTable;
    int entryQuantity = elfDescriptor->symbolTableEntries;
    for(int index = 0; index < entryQuantity; index++){
      int comparison =
        strcmp(symbolName, symbolNameTable + symbolTable[index].st_name);
      if(comparison == 0){
        *address = symbolTable[index].st_value;
        *size = symbolTable[index].st_size;
        return 0;
      }
    }
    *address = 0;
    *size = 0;
    return -1;
  }
