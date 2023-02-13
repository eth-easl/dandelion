// Implementing
#include "compartment.h"
// System Headers
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h> // needs to be before others
#include <sys/cpuset.h>
#include <sys/mman.h>
#include <sys/module.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries
#include "context.h"

// assembly import
#define WRAPPER_SIZE 5
#define WRAPPER_BYTES (WRAPPER_SIZE * 4)
extern void wrapperCode();

// cheri permissions
static const int codePermissions =    0x08002;
static const int memoryPermissions =  0x34000;

// helper function to do syscall on specic core
void* cheriSetup(void* coreIdPointer){
  size_t coreId = *(size_t*)coreIdPointer;
  cpuset_t initialMask;
  cpuset_getaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(initialMask), &initialMask);
  if(!CPU_ISSET(coreId, &initialMask)){
    return (void*)-1;
  }
  cpuset_t threadMask;
  CPU_SETOF(coreId, &threadMask);
  cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(threadMask), &threadMask);
  int syscallId = modfind("dandelionCPUSettings");
  if(syscallId == -1){
    printf("could not find dandelionCPUSettings\n");
    return (void*)-1;
  }
  printf("syscallId %d\n", syscallId);
  struct module_stat stat;
  stat.version = sizeof(stat);
  int syserr =  modstat(syscallId, &stat);
  if(0 != syserr){
    printf("could not stat id\n");
    return (void*)-1;
  }
  int syscall_num = stat.data.intval;
  if(0 != syscall(syscall_num)){
    printf("Syscall failed with: %s\n", strerror(errno));
    return (void*)-1;
  }
  return 0;
}

int sandboxingInit(){
  // run on all different cores once
  // printf("Can access %d CPUs\n", CPU_COUNT(&cpuMask));
  // cpuset_t cpuNew;
  // CPU_SETOF(CPU_FFS(&cpuMask)-1, &cpuNew);
  // printf("Can access %d CPUs\n", CPU_COUNT(&cpuNew));
  // cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, -1, sizeof(cpuNew), &cpuNew);
  // int syscallId = modfind("sys/dandelionCPUSettings");
  // if(syscallId == -1){
  //   printf("could not find dandelionCPUSettings\n");
  //   return -1;
  // }
  // struct module_stat stat;
  // stat.version = sizeof(stat);
  // int syserr =  modstat(syscallId, &stat);
  // if(syserr != 0){
  //   printf("could not stat id\n");
  //   return -1;
  // }
  // syscall(stat.data.intval);
  // find number of available cores
  cpuset_t processMask;
  cpuset_getaffinity(CPU_LEVEL_WHICH, CPU_WHICH_PID, -1, sizeof(processMask), &processMask);
  int coreCount = CPU_COUNT(&processMask);
  // create thread and return value arrays
  pthread_t* threads = calloc(coreCount, sizeof(pthread_t));
  size_t* coreIds = calloc(coreCount, sizeof(size_t));
  void** returnValues = calloc(coreCount, sizeof(int));
  for(int core = 0; core < coreCount; core++){
    // find available core
    coreIds[core] = CPU_FFS(&processMask)-1;
    // remove core from set
    CPU_CLR(coreIds[core], &processMask);
    if(0 != pthread_create(&threads[core], NULL, cheriSetup, &coreIds[core])){
      printf("Failed to create thread for core %zu\n", coreIds[core]);
      return -1;
    }
  }
  int retval = 0;
  for(int core = 0; core < coreCount; core++){
    if(0 != pthread_join(threads[core], &returnValues[core])){
      printf("Failed to join thread for core %zu\n", coreIds[core]);
      retval = -1;
    } else if(0 != (size_t)returnValues[core]){
      printf("Thread for core %zu failed\n", coreIds[core]);
      retval = -1;
    }
  }

  free(threads);
  free(coreIds);
  free(returnValues);
  return retval;
}


/* 
  Side Effect: Cleans up any configurations made by sandboxing
*/
int sandboxingCleanUp(){
  printf("%s; noop, not yet implemented\n", __func__);
  return 0;
}

size_t sandboxSizeRounding(size_t size){
  __asm__ volatile ("RRLEN %0, %0" : "+r"(size));
  return size;
}
// TODO optimize
unsigned char sandboxSizeAlignment(size_t size){
  size_t mask;
  __asm__ volatile("RRMASK %0, %1" : "+r"(mask) : "r"(size));
  int allignment = 63;
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
  return allignment;
}

void sandboxedCall(
  void* functionCode,
  size_t codeSize,
  size_t entryPointOffset,
  char* functionMemory,
  size_t memorySize,
  size_t returnPairOffset,
  void* functionStackPointer
){
  __label__ returnLabel;
  // TODO debug checks for proper alignment
  
  // set up capability for code
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, (unsigned long)functionCode);
  pcc = __builtin_cheri_bounds_set(pcc, codeSize);
  pcc = pcc + entryPointOffset;
  // restrict capability to only be fetch, and executive (executive could be removed in future)
  const unsigned long pccPermissionMask = ~(codePermissions);
  __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(pcc) : "r"(pccPermissionMask));
  // set up capability for memory
  void* __capability ddc = (__cheri_tocap void*__capability) functionMemory;
  ddc = __builtin_cheri_bounds_set(ddc, memorySize);
  const unsigned long ddcPermissionMask = ~(memoryPermissions);
  __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(ddc) : "r"(ddcPermissionMask));
  // allocate space for context
  char* __capability * __capability contextSpaceCap =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*NUM_REGS);

  // allocate return pair
  char* __capability * __capability returnPair =
  (__cheri_tocap char* __capability * __capability) malloc(sizeof(char*__capability)*2);

  // make a return capability
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile (
    "cvtp %x[returnCap], %x[returnAddress] \n"
    : [returnCap] "+r" (returnCap)
    : [returnAddress] "r" (returnAddress)
  );

  // prepare and seal return pair
  returnPair[0] = (char* __capability) contextSpaceCap;
  returnPair[1] = returnCap;
  __asm__ volatile (
    "seal %x[returnPair], %x[returnPair], lpb \n"
    : [returnPair] "+r" (returnPair)
  );
  *((char* __capability *) (functionMemory + returnPairOffset))
    = (char* __capability) returnPair;

  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(ddc, functionStackPointer, pcc);

  returnLabel:
  // restore context needs context cap in c0.
  restoreContext();

  free((__cheri_fromcap void*) returnPair);
  free((__cheri_fromcap void*) contextSpaceCap);

  return;
}
