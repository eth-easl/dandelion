#ifndef DANDELION_FUNCTIONINTERFACE_DANDELIONIO_H_
#define DANDELION_FUNCTIONINTERFACE_DANDELIONIO_H_

// System Headers
#include <stdio.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries

int getInputNumber(void);
size_t getInputSize(unsigned int index);
void* getInputPointer(unsigned int index);

int getOutputNumber(void);
int addOutput(void);
int setOutputSize(unsigned int index, size_t size);
size_t getOutputSize(unsigned int index);
int setOutputPointer(unsigned int index, void* address);
void* getOutputPointer(unsigned int index);

void function_exit(void);

#endif // DANDELION_FUNCTIONINTERFACE_DANDELIONIO_H_
