
// System Headers
#include <sys/param.h>
#include <sys/proc.h>
#include <sys/module.h>
#include <sys/sysproto.h>
#include <sys/sysent.h>
#include <sys/kernel.h>
#include <sys/systm.h>
#include </usr/include/stdint.h>
// Standard Libraries

// Project External Libraries

// Project Internal Libraries

// function to set
static int dandelionCPUSettings(struct thread* td, void* arg){
  printf("enabling pcc bound offset for EL0\n");
  printf("enabling ddc bound offset for EL0\n");
  uint64_t flags = 0;
  // load the previous flags
  __asm__ volatile("mrs %0, CCTLR_EL0" : "=r"(flags));
  // flag for setting pointer access to code to be offset by pcc
  flags = flags | (1L << 3);
  // flag for offsetting pointer access to data to be offset by ddc
  flags = flags | (1L << 2);
  __asm__ volatile("msr CCTLR_EL0, %0" : : "r"(flags));
  return 0;
}

static struct sysent dandelionCPUSettingsSysent = {
  .sy_narg = 0,
  .sy_call = dandelionCPUSettings
};

static int dandelionCPUSettingsOffset = NO_SYSCALL;

static int dandelionCPUSettingsLoad(struct module *module, int event_type, void *arg) {

  int retval = 0; // function returns an integer error code, default 0 for OK

  switch (event_type) { // event_type is an enum; let's switch on it
    case MOD_LOAD:                  // if we're loading
      uprintf("dandelionCPUSettings Loaded at %d\n", dandelionCPUSettingsOffset);      // spit out a loading message
      break;

    case MOD_UNLOAD:                // if were unloading
      uprintf("dandelionCPUSettings Unloaded\n");    // spit out an unloading messge
      break;

    default:
      retval = EOPNOTSUPP;
      break;
  }

  return(retval);                   // return the appropriate value

}

// Register the module with the kernel using:
//  the module name
//  our recently defined moduledata_t struct with module info
//  a module type (we're daying it's a driver this time)
//  a preference as to when to load the module
SYSCALL_MODULE(dandelionCPUSettings, &dandelionCPUSettingsOffset, &dandelionCPUSettingsSysent, dandelionCPUSettingsLoad, NULL);
