# Compartment

In this folder is all the code to set up new compartments from function code
and run compartments later on.

## Compartment Goals
A compartment is a pair of code to be run and a data space available to it.
We want to guarantee that:
- The code can only read and write data from the data space explicitly handed to it
- The data space is initially clean and no capabilities are leaked to the compartment
- The code can't be modified
- If possible the code can't be read

## Architecture
The compartment consists of:
- User code
- Prepended helper code for compatibility
- User memory space

## Assumptions
We assume the following:
- On compartment entry the compartment has only access to capabilities that it is supposed to have access to
  - The registers and memory were sanitized before entry
