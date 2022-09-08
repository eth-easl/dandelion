// Implementing headers
#include "functionManagement.h"
// System Headers

// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "elfParsing.h"

static functionNode_t* rootNode = NULL;

// TODO
// overhault structure
// automatic free of same Id
// and make concurrent?
int addNode(int id, environment_t envType, void* env){
  // allocate new node
  functionNode_t* newNode = malloc(sizeof(functionNode_t));
  newNode->functionId = id;
  newNode->envType = envType;
  newNode->functionEnvironment = env;
  // find place to insert
  functionNode_t* nextNode = rootNode;
  while(nextNode != NULL && nextNode->functionId < id){
    nextNode = nextNode->next;
  }
  newNode->next = nextNode->next;
  nextNode->next = newNode;

  return 0;
}

int removeNode(int id){
  functionNode_t* nextNode = rootNode;
  functionNode_t* prevNode = NULL
  while(nextNode != NULL && nextNode->functionId != id){
    prevNode = nextNode;
    nextNode = nextNode->next;
  }
  // no such node to remove
  if(nextNode == NULL)
    return EINVAL;
  // the node to be romeved was the only one
  if(prevNode == NULL)
    rootNode == NULL;
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
int addFunctionFromStaticElf(int fileDescriptor, size_t maxMemory){
  staticFunctionEnvironment_t* newNode = malloc(sizeof(staticFunctionEnvironment_t));
  *newNode = {};
  newNode->memorySize = maxMemory;

  // read the elf file
  elfDescriptor elf = {};
  if(populateElfDescriptor(elf_file, &elf) != 0){return -1;}

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
  void* functionCode = malloc(topVirtualAddress);
  Elf_Addr topVirtualAddress = 0;
  for (size_t headerIndex = 0; headerIndex < elf.elfHeader.e_shnum; headerIndex++) {
    if(elf.programHeaderTable[headerIndex].p_type == PT_LOAD){
      Elf_Phdr loadedHeader = elf.programHeaderTable[headerIndex];
      Elf_Addr virtualAddress = loadedHeader.p_vaddr;
      Elf_Word loadSize = loadedHeader.p_filesz;
      Elf_Off fileOffset = loadedHeader.p_offset;
      pread(elf_file, functionCode+virtualAddress, loadSize, fileOffset);
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
  newNode->outputNumberOffset = maxOutputNumberAddress;

  return addNode(newNode);
}

int runFunction(int id, void* input, void* output){
  functionNode_t* node = getNode();
  if(node == NULL)
    return EINVAL;
  switch (node->envType) {
    case staticElf:
      return runStaticFunction(node->env, input, output);
    // panic for unkown type
    default:
      return -1;
  }
}

int runStaticFunction(staticFunctionEnvironment_t* env, void* input, void* output){
  // allocate the entire memory

  // copy over the code section

  // add inputs

  // set up outputs
  // prepare outputs
}
