#include <linux/module.h> /* Needed by all modules */
#include <linux/printk.h> /* Needed for pr_info() */
#include <linux/smp.h>    /* Needed to run on all cores */

__UINT64_TYPE__ default_flags = 0;

void dandelion_core_setup(void* info) {
  __UINT64_TYPE__ flags;
  unsigned int proc_id = smp_processor_id();
  flags = default_flags;
  flags |= (1L << 3) | (1L << 2);
  __asm__ volatile("msr CCTLR_EL0, %0" : : "r"(flags));
  pr_info("Dandelion Module loaded on core %d, flags: %lx\n", proc_id, flags);
  return;
}

void dandelion_core_restore(void* info) {
  pr_info("Dandelion Module unloaded on core %d\n", 0);
  __asm__ volatile("msr CCTLR_EL0, %0" : : "r"(default_flags));
  return;
}

int init_module(void) {
  // read default flags on initial core
  __asm__ volatile("mrs %0, CCTLR_EL0" : "=r"(default_flags));
  pr_info("Default flags: %lx\n", default_flags);
  on_each_cpu(dandelion_core_setup, NULL, 1);
  pr_info("Dandelion Module loaded\n");
  return 0;
}

void cleanup_module(void) {
  pr_info("Dandelion Module unloaded\n");
  return;
}

MODULE_LICENSE("GPL");