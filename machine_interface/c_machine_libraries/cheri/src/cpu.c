#include "cpu.h"

#include <pthread.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>

struct thread_state {
  jmp_buf jump_buffer;
  char* __capability register_state[NUM_REGS];
  char* __capability return_pair[2];
};

static pthread_key_t key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

static void interrupt_handler(int sigNumber, siginfo_t* sigInfo,
                              void* context) {
  // write(STDERR_FILENO, "signal handler invoked\n", 24);
  // struct thread_state* current_state = pthread_getspecific(key);
  // //   if there is no thread state returning would immediately reenter
  // // handler
  // // need to reset to default handler for that case
  // if (current_state == NULL) {
  //   struct sigaction default_action = {
  //       .sa_handler = SIG_DFL,
  //   };
  //   sigaction(sigNumber, &default_action, NULL);
  // } else {
  //   longjmp(current_state->jump_buffer, 1);
  // }
  // return;
}

const static struct sigaction catcher = {
    .sa_sigaction = interrupt_handler,
    .sa_flags = SA_RESETHAND | SA_SIGINFO,
    .sa_mask = 0,
};

static void key_init() { pthread_key_create(&key, NULL); }

int cheri_setup() {
  int err = pthread_once(&key_once, key_init);
  // set up storage for error handling
  if (err == 0) {
    struct thread_state* previous_state = pthread_getspecific(key);
    if (previous_state == NULL) {
      // malloc all space that the thread will ever need
      // jump buffer, the register state and the return pair
      struct thread_state* thread_space = malloc(sizeof(struct thread_state));
      err = pthread_setspecific(key, thread_space);
      if (err != 0) return err;
    }
  }
  // install signal handlers for hardware exceptions that can be cause by
  // sandboxed code
  //  SIGBUS, SIGEMT, SIGFPE, SIGILL, SIGSEGV, and SIGTRAP
  // SIGPROT is the cheri exception
  // if ((err = sigaction(SIGSEGV, &catcher, NULL)) != 0) return err;
  // if ((err = sigaction(SIGILL, &catcher, NULL)) != 0) return err;
  if ((err = sigaction(SIGPROT, &catcher, NULL)) != 0) return err;
  // 34 is the cheri capability error
  return err;
}

int cheri_tear_down() {
  void* ptr;
  if ((ptr = pthread_getspecific(key)) != NULL) {
    free(ptr);
  }
  return SUCCESS;
}

char cheri_run_static(cheri_context* context, size_t entry_point,
                      size_t return_pair_offset, size_t stack_pointer) {
  void* __capability context_cap = context->cap;
  // set up capability for code
  unsigned long start_pointer = __builtin_cheri_base_get(context_cap);
  unsigned long size = __builtin_cheri_length_get(context_cap);
  void* __capability pcc = __builtin_cheri_program_counter_get();
  pcc = __builtin_cheri_address_set(pcc, start_pointer);
  pcc = __builtin_cheri_bounds_set(pcc, size);
  pcc = pcc + entry_point;
  // restrict capability to only be fetch,
  // and executive(executive could be removed in future)
  const unsigned long pccPermissionMask = ~(code_permissions);
  // __asm__ volatile("clrperm %w0, %w0, %1" : "+r"(pcc) :
  // "r"(pccPermissionMask));
  return cheri_execute(context_cap, pcc, return_pair_offset,
                       (void*)stack_pointer);
}

char cheri_execute(char* __capability memory, void* __capability function,
                   size_t return_pair_offset, void* stack_pointer) {
  __label__ returnLabel;
  // get thread specific space
  struct thread_state* thread_space = pthread_getspecific(key);
  if (thread_space == NULL) return NO_SETUP;
  if (setjmp(thread_space->jump_buffer) != 0) return INTERRUPTED;

  char* __capability* __capability contextSpaceCap =
      (__cheri_tocap char* __capability* __capability)
          thread_space->register_state;
  char* __capability* __capability returnPairCap =
      (__cheri_tocap char* __capability* __capability)thread_space->return_pair;

  // make a return capability
  char* returnAddress = &&returnLabel;
  char* __capability returnCap;
  __asm__ volatile("cvtp %x[returnCap], %x[returnAddress] \n"
                   : [returnCap] "+r"(returnCap)
                   : [returnAddress] "r"(returnAddress));

  // prepare and seal return pair
  returnPairCap[0] = (char* __capability)contextSpaceCap;
  returnPairCap[1] = returnCap;
  __asm__ volatile("seal %x[returnPairCap], %x[returnPairCap], lpb \n"
                   : [returnPairCap] "+r"(returnPairCap));
  char* memory_pointer = (__cheri_fromcap char*)memory;
  *((char* __capability*)(memory_pointer + return_pair_offset)) =
      (char* __capability)returnPairCap;
  // store current context
  storeContext(contextSpaceCap);
  // clean up context and jump
  prepareContextAndJump(memory, stack_pointer, function);

returnLabel:
  // restore context needs context cap in c0.
  restoreContext();

  return SUCCESS;
}