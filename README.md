# dandelion

This project is aiming to build a workernode for serverless computing with
minimal, secure isolation.

## Build
Before cross compilation can be used you need to fill in the repsective paths
in the toolchain file.
After specifying them you can make a build folder, change in to it, and start
building with the toolchain file:
```
mkdir build
cd build
cmake -DCMAKE_TOOLCHAIN_FILE=../morello-toolchain.txt ..
make
```
For native builds the toolchain specification can be omitted.
