// Implementing headers
#include "functionManagement.h"
// System Headers
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
// Standard Libraries
#include <sys/mman.h>
// Project External Libraries

// Project Internal Libraries
#include "elfParsing.h"
#include "compartment.h"

static functionNode_t* rootNode = NULL;

// TODO
// overhault structure
// automatic free of same Id
// and make concurrent?
int addNode(int id, environment_t envType, void* env){
  // allocate new node
  functionNode_t* newNode = malloc(sizeof(functionNode_t));
  if(newNode == NULL) return -1;
  newNode->functionId = id;
  newNode->envType = envType;
  newNode->functionEnvironment = env;
  // find place to insert
  functionNode_t* currentNode = rootNode;
  if(currentNode == NULL){
    rootNode = newNode;
    return 0;
  }
  if(id < currentNode->functionId){
    newNode->next = rootNode;
    rootNode = newNode;
    return 0;
  }
  while(currentNode->next != NULL && currentNode->next->functionId < id){
    currentNode = currentNode->next;
  }
  newNode->next = currentNode->next;
  currentNode->next = newNode;
  return 0;
}

int removeNode(int id){
  functionNode_t* nextNode = rootNode;
  functionNode_t* prevNode = NULL;
  while(nextNode != NULL && nextNode->functionId != id){
    prevNode = nextNode;
    nextNode = nextNode->next;
  }
  // no such node to remove
  if(nextNode == NULL)
    return EINVAL;
  // the node to be romeved was the only one
  if(prevNode == NULL)
    rootNode = NULL;
  else
    prevNode->next = nextNode->next;
  // free the environment struct
  free(nextNode->functionEnvironment);
  free(nextNode);
  return 0;
}

functionNode_t* getNode(int id){
  functionNode_t* returnNode = rootNode;
  while(returnNode != NULL && returnNode->functionId  != id){
    returnNode = returnNode->next;
  }
  // either the return node has the correct id or the value is NULL
  return returnNode;
}

// TODO add error checking
int addFunctionFromStaticElf(int id, int fileDescriptor, size_t maxMemory,
  int maxOutputs){
  staticFunctionEnvironment_t* newNode = malloc(sizeof(staticFunctionEnvironment_t));
  *newNode = (staticFunctionEnvironment_t){};
  // rount up max memory size to the next bigger representable one
  __asm__ volatile ("RRLEN %0, %0" : "+r"(maxMemory));
  newNode->memorySize = maxMemory;

  // read the elf file
  elfDescriptor elf = {};
  if(populateElfDescriptor(fileDescriptor, &elf) != 0){return -1;}
  newNode->entryPoint = elf.elfHeader.e_entry;
  // determine size needed for code
  Elf_Addr topVirtualAddress = 0;
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
    }
  }

  newNode->codeSize = topVirtualAddress;
  // allocated memory for code and load it from elf file
  void* functionCode = calloc(1,topVirtualAddress);
  for (size_t headerIndex = 0; headerIndex < elf.elfHeader.e_shnum; headerIndex++) {
    if(elf.programHeaderTable[headerIndex].p_type == PT_LOAD){
      Elf_Phdr loadedHeader = elf.programHeaderTable[headerIndex];
      Elf_Addr virtualAddress = loadedHeader.p_vaddr;
      Elf_Word loadSize = loadedHeader.p_filesz;
      Elf_Off fileOffset = loadedHeader.p_offset;
      pread(fileDescriptor, functionCode+virtualAddress, loadSize, fileOffset);
    }
  }
  newNode->pcc = functionCode;

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
  newNode->returnPairOffset = returnPairAddress;
  newNode->inputRootOffset = inputRootAddress;
  newNode->inputNumberOffset = inputNumberAddress;
  newNode->outputRootOffset = outputRootAddress;
  newNode->outputNumberOffset = outputNumberAddress;
  newNode->maxOutputNumberOffset = maxOutputNumberAddress;
  // set the max output number
  *((int*)((char*) functionCode + maxOutputNumberAddress)) = maxOutputs;
  int retval = addNode(id, staticElf, newNode);
  freeElfDescriptor(&elf);
  return retval;
}

int runFunction(
  int id,
  ioStruct* input, int inputNumber,
  ioStruct** output, int* outputNumber){
  functionNode_t* node = getNode(id);
  if(node == NULL)
    return EINVAL;
  switch (node->envType) {
    case staticElf:
      return runStaticFunction(node->functionEnvironment,
        input, inputNumber,
        output, outputNumber);
    // panic for unkown type
    default:
      return -1;
  }
}

int runStaticFunction(staticFunctionEnvironment_t* env,
  ioStruct* input, int inputNumber,
  ioStruct** output, int* outputNumber){
  // allocate the entire memory
  size_t mask;
  int allignment = 63;
  __asm__ volatile("RRMASK %0, %1" : "+r"(mask) : "r"(env->memorySize));
  // find position of most significant 0
  while(allignment > 0){
    if(!((1L << allignment) & mask)){
      break;
    }
    allignment--;
  }
  // gives the left shift to first non 0, this means last 1 is +2
  // 1 for correcting that we detect the first 0
  // and 1 for accounting the original 1
  allignment += 2;

  void* functionMemory = mmap(NULL,
    env->memorySize,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS | MAP_ALIGNED(allignment), // get anonymous, page alligned memory
    -1,
    0
  );
  // copy over the code section
  void* funcMem = memcpy(functionMemory, env->pcc, env->codeSize);
  // TODO check if it is still correct

  size_t codeEnd = env->codeSize;
  ioStruct** inputRootPointer = (ioStruct**)((char*) functionMemory + env->inputRootOffset);
  int* inputNumberPointer = (int*)((char*) functionMemory + env->inputNumberOffset);
  ioStruct** outputRoot = (ioStruct**)((char*) functionMemory + env->outputRootOffset);
  int* maxOutputNumber = (int*)((char*) functionMemory + env->maxOutputNumberOffset);
  *inputRootPointer = (void*) codeEnd;
  ioStruct* inputRoot = (ioStruct*)((char*) functionMemory + codeEnd);
  codeEnd += sizeof(ioStruct)*inputNumber;
  *inputNumberPointer = inputNumber;
  *outputRoot = (void*) codeEnd;
  codeEnd += sizeof(ioStruct)* (*maxOutputNumber);
  // copy inputs
  for(int inputIndex = 0; inputIndex < inputNumber; inputIndex++){
    // copy data
    void* errCheck = memcpy((char*) functionMemory + codeEnd,
      input[inputIndex].address, input[inputIndex].size);
    // set up the environment
    inputRoot[inputIndex].address = (void*) codeEnd;
    inputRoot[inputIndex].size = input[inputIndex].size;
    // TODO check if correct
    codeEnd += input[inputIndex].size;
  }
  // set up ddc and pcc
  void* __capability ddc = (__cheri_tocap void*__capability) functionMemory;
  ddc = __builtin_cheri_bounds_set(ddc, env->memorySize);
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionMemory);
  pcc = __builtin_cheri_bounds_set(pcc, env->memorySize);
  pcc = pcc + env->entryPoint;
  // make call to function
  sandboxedCall(pcc, ddc, env->returnPairOffset, (void*) env->memorySize);
  // keep outputs
  *outputNumber = *(int*)((char*)functionMemory + env->outputNumberOffset);
  *output = calloc(*outputNumber,sizeof(ioStruct));
  for(int outIndex = 0; outIndex < *outputNumber; outIndex++){
    ioStruct* current = (ioStruct*)((char*)functionMemory + (size_t)*outputRoot);
    (*output)[outIndex].size = current->size;
    (*output)[outIndex].address = calloc(1, current->size);
    memcpy((*output)[outIndex].address,
      ((char*)functionMemory + (size_t)current->address), current->size);
    // TODO check that it worked

  }
  // unmap function memory
  munmap(functionMemory, env->memorySize);
  // TODO check for errors

  return 0;
}
