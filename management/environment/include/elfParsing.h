#ifndef DANDELION_MANAGEMENT_ENVIRONMENT_ELFPARSING_H_
#define DANDELION_MANAGEMENT_ENVIRONMENT_ELFPARSING_H_

// System Headers
#include <elf.h>

// Standard Libraries

// Project External Libraries

// Project Internal Libraries

typedef struct{
  Elf_Ehdr elfHeader;
  Elf_Phdr* programHeaderTable;
  Elf_Shdr* sectionHeaderTable;
  char* sectionNameTable;
  Elf_Sym* symbolTable;
  int symbolTableEntries;
  char* symbolNameTable;
} elfDescriptor;

/*
  Input:
    Open elf file
  Output:
    Descriptor struct with all the tables loaded into memory for easy access
*/
int populateElfDescriptor(int file, elfDescriptor* descriptor);

/*
  Input:
    Elf descriptor that has been populated.
  Sideeffects:
    Free all tables that are dynamically allocated.
*/
void freeElfDescriptor(elfDescriptor* desc);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
  Sideeffects:
    Fill in the elf header
*/
int populateElfHeader(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
    Needs elf header to already be populated
  Sideeffects:
    Allocate memory for and load program header table
*/
int populateProgramHeaderTable(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
    Needs elf header to already be populated
  Sideeffects:
    Allocate memory for and load section header table
*/
int populateSectionHeaderTable(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
    Needs elf header to already be populated
  Sideeffects:
    Allocate memory for and load symbol name table
*/
int populateSectionNameTable(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
    Needs elf header and section header table to be populated already
  Sideeffects:
    Allocate memory for and load symbol table
*/
int populateSymbolTable(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    Open Elf file descriptor
    Pointer to elf descriptor to pupulate
    Needs elf header and section header table to be populated already
  Sideeffects:
    Allocate memory for and load symbol name table
*/
int populateSymbolNameTable(int elfFile, elfDescriptor* elfDescriptor);

/*
  Input:
    populated elf descriptor
    char pointer to a null terminated string with the simbol name to search for
    maximum length of the symbol name
  Output:
    writes virtual address of symbol to address (virtual address means the
    address of the symbol after loading the elf as specified)
*/
int getSymbolAddress(elfDescriptor* elfDescriptor, const char* symbolName,
  Elf_Addr* address, Elf_Word* size);

#endif
