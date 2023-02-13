// include for printing
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/module.h>
#include <sys/mman.h>
#include <sys/cpuset.h>
// include to use cheribsd dynamic loader
#include <fcntl.h>
#include <elf.h>
#include <err.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
// internal includes
#include "compartment.h"
#include "elfParsing.h"
#include "dandelionIOInternal.h"
#include "functionManagement.h"
#include "mongoose.h"


// global variables
size_t bodyLength = 4;

int size = 128;

// struct request{
//   struct request* next;
//   void (*handler)(struct mg_connection*);
//   struct mg_connection* c;
// };

// static struct request* head = NULL;
// static struct request* tail = NULL;

// static pthread_mutex_t queueMutex;
// static pthread_cond_t queueCondVariable;

#define WORKER_NUM 128
const int workerNum = WORKER_NUM;
pthread_t workerThreads[WORKER_NUM];
struct mg_mgr mgr[WORKER_NUM];
int workerIds[WORKER_NUM];

// void putRequest(struct request* req){
//   pthread_mutex_lock(&queueMutex);
//   if(tail != NULL){
//     tail->next = req;
//   }
//   if(head == NULL){
//     head = req;
//   }
//   tail = req;
//   pthread_mutex_unlock(&queueMutex);
//   pthread_cond_signal(&queueCondVariable);
// }

// struct request* getRequest(void){
//   pthread_mutex_lock(&queueMutex);
//   while(head == NULL){
//     pthread_cond_wait(&queueCondVariable, &queueMutex);
//   }
//   struct request* retVal = head;
//   head = head->next;
//   pthread_mutex_unlock(&queueMutex);
//   return retVal;
// }

static void serveHot(struct mg_connection *connection){
  int matSize = size*size;
  int* matDim = malloc(sizeof(int));
  if(matDim == NULL){
    mg_http_reply(connection, 500, "", "%s; Could not allocate matDim\n", __func__);
  }
  *matDim = size;
  int64_t* inputMat = malloc(matSize*sizeof(int64_t));
  if(inputMat == NULL){
    mg_http_reply(connection, 500, "", "%s; Could not allocate inputMat\n", __func__);
    free(matDim);
  }
  for (int i = 0; i < matSize; i++){
    inputMat[i] = i+1;
  }
  ioStruct inputStruct[2];
  inputStruct[0].size = sizeof(int);
  inputStruct[0].address = matDim;
  inputStruct[1].size = sizeof(int64_t)*matSize;
  inputStruct[1].address = inputMat;
  ioStruct* outputStruct;
  int outputCount;
  runFunction(1, inputStruct, 2, &outputStruct, &outputCount);

  // check output
  // printf("Function has %d outputs\n", outputCount);
  // if(outputCount == 1 && outputStruct[0].size == sizeof(int64_t)*matSize){
  //   int64_t* outMat = outputStruct[0].address;
  //   for(int i = 0; i < size; i++){
  //     for(int j = 0; j < size; j++){
  //       printf("%ld ", outMat[i*size + j]);
  //     }
  //     printf("\n");
  //   }
  // }

  // free all inputs and outputs
  free(matDim);
  free(inputMat);
  for(int outNum = 0; outNum < outputCount; outNum++){
    free(outputStruct[outNum].address);
  }
  mg_http_reply(connection, 200, "", "Done: Hot \n");
  return;
}

static void serveCold(struct mg_connection *connection){
  // load it from file
  int elf_file = open("wrapper", O_RDONLY | O_DIRECT);
  staticFunctionEnvironment_t* coldNode = 
    getStaticNode(elf_file, 1<<22, 1);
  if(coldNode == NULL){
    mg_http_reply(connection, 500, "", "%s; Could not load from elf file\n", __func__);
    return;
  }
  // prepare input
  int matSize = size*size;
  int* matDim = malloc(sizeof(int));
  if(matDim == NULL){
    mg_http_reply(connection, 500, "", "%s; Could not allocate matDim\n", __func__);
    close(elf_file);
  }
  *matDim = size;
  int64_t* inputMat = malloc(matSize*sizeof(int64_t));
  if(inputMat == NULL){
    mg_http_reply(connection, 500, "", "%s; Could not allocate inputMat\n", __func__);
    close(elf_file);
    free(matDim);
  }
  for (int i = 0; i < matSize; i++){
    inputMat[i] = i+1;
  }
  ioStruct inputStruct[2];
  inputStruct[0].size = sizeof(int);
  inputStruct[0].address = matDim;
  inputStruct[1].size = sizeof(int64_t)*matSize;
  inputStruct[1].address = inputMat;
  ioStruct* outputStruct;
  int outputCount;
  // run function
  if(0 != runStaticFunction(coldNode, inputStruct, 2, &outputStruct, &outputCount)){
    mg_http_reply(connection, 500, "", "%s; Failed to run cold function\n", __func__);
    free(matDim);
    free(inputMat);
    free(coldNode->pcc);
    free(coldNode);
    return;
  }
  // process output
  // printf("Function has %d outputs\n", outputCount);
  // if(outputCount == 1 && outputStruct[0].size == sizeof(int64_t)*matSize){
  //   int64_t* outMat = outputStruct[0].address;
  //   for(int i = 0; i < size; i++){
  //     for(int j = 0; j < size; j++){
  //       printf("%ld ", outMat[i*size + j]);
  //     }
  //     printf("\n");
  //   }
  // }
  // free memory
  free(matDim);
  free(inputMat);
  for(int outNum = 0; outNum < outputCount; outNum++){
    free(outputStruct[outNum].address);
  }
  free(coldNode->pcc);
  free(coldNode);
  close(elf_file);
  mg_http_reply(connection, 200, "", "Done: Cold\n");
  return;
}

static void* workerThread(void* args){
  int workerId = *(int*)args;
  // printf("Listening to %s\n", listenAddress);
  for (;;) mg_mgr_poll(&mgr[workerId], 1000);
  // while(1){
  //   struct request* req = getRequest();
  //   req->handler(req->c);
  //   free(req);
  // }
  return NULL;
}

static void requestHandler(struct mg_connection *c, int ev, void *ev_data, void *fn_data) {
  if(ev == MG_EV_HTTP_MSG) {
    struct mg_http_message *hm = (struct mg_http_message *) ev_data;
    if(mg_http_match_uri(hm, "/hot")){
      // struct request* req = malloc(sizeof(struct request));
      // req->handler = serveHot;
      // req->c = c;
      // req->next = NULL;
      // putRequest(req);
      serveHot(c);
    } else if (mg_http_match_uri(hm, "/cold")){
      // struct request* req = malloc(sizeof(struct request));
      // req->handler = serveCold;
      // req->c = c;
      // req->next = NULL;
      // putRequest(req);
      serveCold(c);
    } else {
      mg_http_reply(c, 200, "", "Hello from Server\n");  
    }
  }
  // if(ev == MG_EV_CLOSE) {
  //   if (c->fn_data != NULL){
  //     int joinError = pthread_join(*(pthread_t*)c->fn_data, NULL);
  //     if(0 != joinError){
  //       printf("Failed join with %d\n", joinError);
  //     }
  //     free(c->fn_data);
  //   }
  // }
}

int main(int argc, char *const argv[]) {
  printf("Server Hello\n");
  int c, errflag = 0;
  int port = 8080;
  while((c = getopt(argc, argv, "p:")) != -1){
    switch (c)
    {
    case 'p':
      port = atoi(optarg);
      if(port == 0){ errflag++; }
      break;
    case '?':
      printf("Unkown flag %c\n", c);
      errflag++;
      break;
    default:
      errflag++;
      break;
    }
  }
  if(errflag != 0) { return -1; }
  // open file
  int elf_file = open("wrapper", O_RDONLY);

  if(addFunctionFromElf(1, elf_file, 1<<22, 1, staticElf) != 0){
    printf("Error when adding function\n");
  }

  // initialize mutex and cond var
  // if(0 != pthread_mutex_init(&queueMutex, NULL)){
  //   printf("Failed to initialize mutex\n");
  //   return -1;
  // }
  // if(0 != pthread_cond_init(&queueCondVariable, NULL)){
  //   printf("Failed to initialize conditional variable\n");
  //   return -1;
  // }

  // allocate 8 worker threads
  // for(int worker = 0; worker < workerNum; worker++){
  //   if(0!= pthread_create(&workerThreads[worker], NULL, workerThread, NULL)){
  //     printf("Failed to create worker %d\n", worker);
  //     return -1;
  //   }
  // }

  const int addressSize = 26;
  int addLen = 0;
  char listenAddress[addressSize];

  for(int worker = 0; worker < workerNum; worker++){
    if(addressSize <= (addLen = snprintf(listenAddress, addressSize,
      "http://0.0.0.0:%d", port+worker))){
      printf("Could not assemble valid address, len: %d, str: %s\n",
        addLen, listenAddress);
      return -1;
    }
    printf("Listening to %s\n", listenAddress);
    mg_mgr_init(&mgr[worker]);
    if(NULL == mg_http_listen(&mgr[worker], listenAddress, requestHandler, NULL)){
      printf("Could not listen to %s\n", listenAddress);
      return -1;
    }
    workerIds[worker] = worker;
    if(0!= pthread_create(&workerThreads[worker], NULL, workerThread, &workerIds[worker])){
      printf("Failed to create worker %d\n", worker);
      return -1;
    }
    // printf("Listening to %s\n", listenAddress);
    // for (;;) mg_mgr_poll(&mgr[worker], 1);
  }
  for(int worker = 0; worker < workerNum; worker++){
    pthread_join(workerThreads[worker],NULL);
  }
  printf("Server Goodbye\n");
  return 0;

}
