#ifndef DANDELION_MANAGEMENT_ENVIRONMENT_FUNCTIONMANAGEMENT_H_
#define DANDELION_MANAGEMENT_ENVIRONMENT_FUNCTIONMANAGEMENT_H_

enum environment_t {staticElf};

typedef struct {
  void* pcc;
  size_t codeSize;
  size_t memorySize;
  Elf_Addr returnPairOffset;
  Elf_Addr inputRootOffset;
  Elf_Addr inputNumberOffset;
  Elf_Addr outputRootOffset;
  Elf_Addr outputNumberOffset;
} staticFunctionEnvironment_t;

typedef struct {
  int functionId;
  environment_t envType;
  void* functionEnvironment;
  functionNode_t* next;
} functionNode_t;

int addNode(int id, environment_t envType, void* env);

int removeNode(int id);

functionNode_t* getNode(int id);

int addFunctionFromStaticElf(int fileDescriptor, int maxOutputs);

int runFunction(int id, void* input, void* output);

int runStaticFunction(staticFunctionEnvironment_t* env, void* input);

#endif
