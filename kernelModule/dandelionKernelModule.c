
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

// original settings
static uint64_t defaultFlags = 0;

static void dandelionCPUSettingsOn() {
  // uprintf("enabling pcc bound offset for EL0\n");
  // uprintf("enabling ddc bound offset for EL0\n");
  uint64_t flags = 0;
  // load the previous flags
  __asm__ volatile("mrs %0, CCTLR_EL0" : "=r"(flags));
  defaultFlags = flags;
  // flag for setting pointer access to code to be offset by pcc
  flags = flags | (1L << 3);
  // flag for offsetting pointer access to data to be offset by ddc
  flags = flags | (1L << 2);
  __asm__ volatile("msr CCTLR_EL0, %0" : : "r"(flags));
}

static void dandelionCPUSettingsOff(){
  uprintf("resetting to default flags\n");
  __asm__ volatile("msr CCTLR_EL0, %0" : : "r"(defaultFlags));
}

// function to set cpu setttings
static int dandelionCPUSettings(struct thread* td, void* arg){
  // struct dandelionSettings* settings = (struct dandelionSettings*) arg;
  printf("Call to %s\n", __func__);
  dandelionCPUSettingsOn();

  // TODO: accept argument to either turn on or off

  // TODO: Currently turns only on on the core this is executed
  // want to change it to turn on on all cores and then the off call to turn on off on all cores
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
