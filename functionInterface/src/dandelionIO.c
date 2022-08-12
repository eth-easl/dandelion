// Implementing
#include "dandelionIO.h"
#include "dandelionIOInternal.h"
// System Headers

// Standard Libraries

// Project External Libraries

// Project Internal Libraries

// type to store meta information about inputs
static ioStruct* const inputRoot;
static ioStruct* outputRoot;
static const unsigned int inputNumber;
static unsigned int outputNumber = 0;
static const unsigned int maxOutputNumber;

int getInputNumber(void){
  return inputNumber;
}
size_t getInputSize(unsigned int index){
  if(index < inputNumber){ return inputRoot[index].size; }
  else { return 0; };
}
void* getInputPointer(unsigned int index){
  if(index < inputNumber){ return inputRoot[index].address; }
  else { return NULL; };
}

int getOutputNumber(void){
  return outputNumber;
}

int addOutput(void){
  if(outputNumber<maxOutputNumber){
    outputNumber++;
    return 0;
  }
  return -1;
}

int setOutputSize(unsigned int index, size_t size){
  if(index < outputNumber){ outputRoot[index].size = size; return 0; }
  else { return -1; }
}

size_t getOutputSize(unsigned int index){
  if(index < outputNumber){ return outputRoot[index].size; }
  else { return 0; }
}

int setOutputPointer(unsigned int index, void* address){
  if(index < outputNumber) { outputRoot[index].address = address; return 0; }
  else { return -1; }
}

void* getOutputPointer(unsigned int index){
  if(index < outputNumber) { return outputRoot[index].address; }
  else { return NULL; }
}
