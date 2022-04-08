// include for printing
#include <stdio.h>
// include to use cheribsd dynamic loader
#include <dlfcn.h>

int main(int argc, char const *argv[]) {
  printf("Server Hello\n");

  // open libary file
  char basicPath[] = "../functions/libbasic.so";
  // set options:
  // all external function references are bound immediately
  int basicMode = RTLD_NOW;

  void* dlHandleBasic = dlopen(basicPath, basicMode);
  // check if loading was successful
  if (dlHandleBasic == NULL){
    char* errorMessage = dlerror();
    printf("Loading of libbasic failed with error: %s\n", errorMessage);
    return -1;
  }

  // find the noOperation function and call it once
  void(*noOperation)() = (void(*)()) dlfunc(dlHandleBasic, "noOperation");
  if (noOperation == NULL){
    char* errorMessage = dlerror();
    printf("Function find failed with error: %s\n", errorMessage);
    return -1;
  } else {
    noOperation();
    printf("noOperation successful\n");
  }

  int(*returnSeven)() = (int(*)()) dlfunc(dlHandleBasic, "returnSeven");
  if(returnSeven == NULL){
    char* errorMessage = dlerror();
    printf("Function find failed with error: %s\n", errorMessage);
    return -1;
  } else {
    int seven = returnSeven();
    if(seven != 7){
      printf("returnSeven not returning 7\n");
    } else {
      printf("returnSeven successful\n");
    }
  }

  int(*returnNumber)(int) = (int(*)(int)) dlfunc(dlHandleBasic, "returnNumber");
  if(returnNumber == NULL){
    char* errorMessage = dlerror();
    printf("Function find failed with error: %s\n", errorMessage);
    return -1;
  } else {
    int eight = returnNumber(8);
    if(eight != 8){
      printf("returnNumber not returning same as input\n");
      printf("input: %d, output: %d\n", 8, eight);
    } else {
      printf("returnNumber successful\n");
    }
  }

  int(*addOne)(int) = (int(*)(int)) dlfunc(dlHandleBasic, "addOne");
  if(addOne == NULL){
    char* errorMessage = dlerror();
    printf("Function find failed with error: %s\n", errorMessage);
    return -1;
  } else {
    int nine = addOne(8);
    if(nine != 9){
      printf("addOne not adding one\n");
      printf("input: %d, output: %d\n", 8, nine);
    } else {
      printf("addOne successful\n");
    }
  }

  printf("server finished\n");

  return 0;
}
