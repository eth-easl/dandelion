// include for printing
#include <stdio.h>
#include <sys/mman.h>

int main(int argc, char const *argv[]) {

  // open the libarary file
  FILE* basicLib = fopen("./libbasic.so","r");
  if(!basicLib){
    printf("could not open basicLib\n");
  }
  // go to the libarary function
  int offset = 0x10670 - 0x105f4 +  0x5f4;
  const int instructions = 6; // 0x18 / 4
  if(fseek(basicLib, offset, SEEK_SET)){
    printf("There was an error with seeking the libarary\n");
  }

  // get some readable, writable and executable memory
  void* functionCode = mmap(NULL,
    instructions*4,
    PROT_EXEC | PROT_READ | PROT_WRITE,
    MAP_ANONYMOUS,
    -1,
    0
  );

  // read the aproprate number of bytes
  fread(functionCode, 4, instructions, basicLib);

  // printf("instructions: \n");
  // for(int i = 0; i < instructions; i++){
  //   printf("%08x\n", ((int*)functionCode)[i]);
  // }

  // make a function pointer from it.
  int(*addOne)(int) = (int(*)(int)) functionCode;

  // call to it
  int testInt = 7;
  testInt = addOne(testInt);

  printf("test_move is now: %d \n", testInt);

  return 0;
}
