#ifndef DANDELION_MANAGEMENT_ENVIRONMENT_FUNCTIONMANAGEMENT_H_
#define DANDELION_MANAGEMENT_ENVIRONMENT_FUNCTIONMANAGEMENT_H_

// System Headers
#include <stdio.h>
#include <elf.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "dandelionIOInternal.h"

typedef enum {staticElf} environment_t;

typedef struct {
  void* pcc;
  size_t codeSize;
  size_t memorySize;
  Elf_Addr entryPoint;
  Elf_Addr returnPairOffset;
  Elf_Addr inputRootOffset;
  Elf_Addr inputNumberOffset;
  Elf_Addr outputRootOffset;
  Elf_Addr outputNumberOffset;
  Elf_Addr maxOutputNumberOffset;
} staticFunctionEnvironment_t;

typedef struct functionNode_t {
  int functionId;
  environment_t envType;
  void* functionEnvironment;
  struct functionNode_t* next;
} functionNode_t;

int addNode(int id, environment_t envType, void* env);

int removeNode(int id);

functionNode_t* getNode(int id);

staticFunctionEnvironment_t* getStaticNode
  (int fileDescriptor, size_t maxMemory, int maxOutputs);

int addFunctionFromElf(int id, int fileDescriptor, size_t maxMemory,
  int maxOutputs, environment_t env);

int runFunction(int id, ioStruct* input, int inputNumber,
  ioStruct** output, int* outputNumber);

int runStaticFunction(staticFunctionEnvironment_t* env,
  ioStruct* input, int inputNumber,
  ioStruct** output, int* outputNumber);

#endif
