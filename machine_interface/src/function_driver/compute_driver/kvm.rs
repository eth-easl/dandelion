use crate::{
    function_driver::{
        functions::{ElfConfig, Function, FunctionConfig},
        load_utils::load_u8_from_file,
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, EngineWorkQueue,
    },
    interface::{read_output_structs, setup_input_structs, write_heap_end},
    memory_domain::{
        kvm::{Page, PageType},
        Context, ContextTrait, ContextType, MemoryDomain,
    },
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core::slice;
use core_affinity;
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult, UserError};
use kvm_bindings::{kvm_clock_data, kvm_userspace_memory_region, KVM_MAX_CPUID_ENTRIES};
use kvm_ioctls::{Kvm, VcpuExit, VcpuFd, VmFd};
use log::{debug, info};
use std::{
    collections::btree_map::Entry,
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
};

#[cfg(target_arch = "x86_64")]
mod x86_64;
#[cfg(target_arch = "x86_64")]
mod x86_64_asm;
#[cfg(target_arch = "x86_64")]
pub use x86_64::PAGE_SIZE;
#[cfg(target_arch = "x86_64")]
use x86_64::*;

#[cfg(target_arch = "aarch64")]
mod aarch64;
#[cfg(target_arch = "aarch64")]
use aarch64::*;

const _: () = assert!(PAGE_SIZE.is_power_of_two());
pub fn round_down_to_page(address: usize) -> usize {
    address & !(PAGE_SIZE - 1)
}

#[cfg(feature = "backend_debug")]
fn dump_memory(data: &[u8]) {
    for (i, &byte) in data.iter().enumerate() {
        if i % 16 == 0 {
            print!("\n{:08x}  ", i);
        }
        print!("{:02x} ", byte);
    }
    println!();
}

#[cfg(feature = "backend_debug")]
fn step_debug(vcpu: &VcpuFd) {
    vcpu.set_guest_debug(&kvm_bindings::kvm_guest_debug {
        control: kvm_bindings::KVM_GUESTDBG_ENABLE | kvm_bindings::KVM_GUESTDBG_SINGLESTEP,
        pad: 0,
        arch: Default::default(),
    })
    .unwrap();
}

/// Address above which the memory is used for common mappings (interrupt handlers, root page table etc.).
/// We don't want to require multiple PML4 entries, so we use the end of the last GiB
/// of the first PML4 entry (total 512 GiB) for the common mapped memory.
const SYSTEM_VIRTUAL_BASE: usize = (1 << 39) - (1 << 30);

/// Amount of memory that is mapped into all VMs for interrupt handlers and root page tables.
/// Allocate a slice. The first 2MB will be mapped to all VMs, the rest can be used
/// the space after can be used for root only data.
/// (Like the root page table)
/// Amount of memory reserved for the system. Allocated at the end of the Physical address space
/// Share memory mappings that all VMs need, such as GDT, IDT, interrupt handlers and the TSS.
/// These mappings can be marked global, as they need to accesible from root and user space and can be used in all VMs.
/// In the same memory slot also have the root page table, that does not get mapped into user space.
const GLOBAL_SYSTEM_MEMORY_SIZE: usize = 5 * PAGE_SIZE;
const ROOT_PAGE_TABLE_SIZE: usize = 2 * PAGE_SIZE;
const SYSTEM_MEMORY_SIZE: usize = GLOBAL_SYSTEM_MEMORY_SIZE + ROOT_PAGE_TABLE_SIZE;

/// Each VM needs to have some memory for per VM tasks that is useful to have at the same physical location everywhere.
/// For example stack space for interrupt handlers.
const PER_VM_SYSTEM_MEMORY: usize = 2 * PAGE_SIZE;

/// Memory that is used across all VMs. Once initialized, contains all interrupt handlers as well as the root page table.
/// Interrupt handlers and tables are expected to be put into pages also accessable from userspace, root page tables not.
/// Also contains a place to coordinated free memory between VMs.
static ROOT_MAPPING: LazyLock<u64> = LazyLock::new(|| {
    let static_start = unsafe {
        std::ptr::from_mut(
            nix::sys::mman::mmap_anonymous(
                None,
                NonZeroUsize::new(SYSTEM_MEMORY_SIZE).unwrap(),
                nix::sys::mman::ProtFlags::all(),
                nix::sys::mman::MapFlags::MAP_SHARED,
            )
            .unwrap()
            .as_mut(),
        )
    } as *mut u8;
    let memory = unsafe { slice::from_raw_parts_mut(static_start, SYSTEM_MEMORY_SIZE) };
    // write the interrupt handlers
    prepare_static_memory(memory);
    static_start.addr() as u64
});

struct KvmLoop {
    vm: VmFd,
    vcpu: VcpuFd,
    state: ResetState,
}

impl EngineLoop for KvmLoop {
    fn init(_core_id: u8) -> DandelionResult<Box<Self>> {
        let kvm = Kvm::new().unwrap();
        assert_eq!(kvm.get_api_version(), 12);

        let vm = kvm.create_vm().unwrap();
        let vcpu = vm.create_vcpu(0).unwrap();

        if kvm.check_extension(kvm_ioctls::Cap::TscControl) {
            vcpu.set_tsc_khz(1_000_000).unwrap();
        }

        #[cfg(target_arch = "x86_64")]
        {
            // enable all features the real cpu has on the vcpu
            let cpuid = kvm.get_supported_cpuid(KVM_MAX_CPUID_ENTRIES).unwrap();
            vcpu.set_cpuid2(&cpuid).unwrap();
            // check that the clock capability is avalilable, so the clock can be set bevore entry
            assert!(kvm.check_extension(kvm_ioctls::Cap::KvmclockCtrl));
        }

        let state = ResetState::new(&vm, &vcpu);

        Ok(Box::new(KvmLoop { vm, vcpu, state }))
    }

    fn get_engine_type(&self) -> crate::machine_config::EngineType {
        crate::machine_config::EngineType::Kvm
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_sets: &[String],
    ) -> DandelionResult<Context> {
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return err_dandelion!(DandelionError::ConfigMissmatch),
        };
        setup_input_structs::<u64, u64>(&mut context, elf_config.system_data_offset, output_sets)?;
        let min_stack_start = context.get_last_item_end();
        let kvm_context = match &mut context.context {
            ContextType::Kvm(kvm_context) => kvm_context,
            _ => return err_dandelion!(DandelionError::ContextMissmatch),
        };
        let stack_start = kvm_context.context_size;
        if min_stack_start >= stack_start {
            return err_dandelion!(DandelionError::UserError(UserError::SmallContext));
        }

        #[cfg(feature = "backend_debug")]
        {
            debug!("entry point: {:#x}", elf_config.entry_point);
        }

        // attach VM memory
        // TODO could do this on VM setup, aso should free the task stack when the loop state is dropped
        unsafe {
            self.vm
                .set_user_memory_region(kvm_userspace_memory_region {
                    slot: 0,
                    flags: 0,
                    guest_phys_addr: 0x0,
                    memory_size: (kvm_context.backing_memory.get_mut_u64().len() * size_of::<u64>())
                        as u64,
                    userspace_addr: kvm_context.backing_memory.get_mut_u64().as_ptr().addr() as u64,
                })
                .unwrap();
            // Page tables need to be writable, as CPU will attempt to set accessed bit.
            self.vm
                .set_user_memory_region(kvm_userspace_memory_region {
                    slot: 1,
                    flags: 0,
                    guest_phys_addr: SYSTEM_VIRTUAL_BASE as u64,
                    memory_size: SYSTEM_MEMORY_SIZE as u64,
                    userspace_addr: *ROOT_MAPPING,
                })
                .unwrap();
            self.vm
                .set_user_memory_region(kvm_userspace_memory_region {
                    slot: 2,
                    flags: 0,
                    guest_phys_addr: (SYSTEM_VIRTUAL_BASE + SYSTEM_MEMORY_SIZE) as u64,
                    memory_size: PER_VM_SYSTEM_MEMORY as u64,
                    userspace_addr: self.state.task_stack.as_mut_ptr() as u64,
                })
                .unwrap();
        }

        #[cfg(target_arch = "x86_64")]
        self.vm.set_clock(&kvm_clock_data::default()).unwrap();

        info!("Elf entry: {}", elf_config.entry_point);
        // initialize vCPU
        self.state.init_vcpu(
            &self.vcpu,
            elf_config.entry_point as u64,
            kvm_context,
            stack_start,
        )?;

        // make sure that the stack start has not moved into the occupied territory
        write_heap_end::<u64, u64>(
            kvm_context,
            elf_config.system_data_offset,
            stack_start as u64,
        )?;

        #[cfg(feature = "backend_debug")]
        {
            // configure single-step debugging
            step_debug(&self.vcpu);
            dump_regs(&self.vcpu);
        }

        // start running the function
        // TODO: on unexpected break, mark function as failure
        loop {
            let reason = self.vcpu.run().unwrap();
            match reason {
                // the expected way to exit a function with the new interface
                VcpuExit::IoOut(32, _) => {
                    break;
                }
                #[cfg(feature = "backend_debug")]
                VcpuExit::IoOut(14, _) => {
                    // ATTENTION: uncomment out 14 in x86_64 page handler so it exits to here
                    // check_page_fault_handling(
                    //     &self.vcpu,
                    //     &page_fault_metadata,
                    //     kvm_context.storage,
                    // );
                    debug!("Exit from page fault handler");
                    dump_regs(&self.vcpu);
                    break;
                }
                VcpuExit::IoOut(1, _) => {
                    debug!("Exit at end of asm");
                    dump_regs(&self.vcpu);
                    break;
                }
                // #[cfg(feature = "backend_debug")]
                VcpuExit::IoOut(point, value) => {
                    let val = value[0];
                    debug!("Exit from handler at point {:?}", point);
                    dump_regs(&self.vcpu);
                    if point == 35 && val == 5 {
                        break;
                    }
                }
                VcpuExit::SystemEvent(_type, _data) => {
                    debug!("System Event, type: {}", _type);
                    break;
                }
                VcpuExit::Debug(info) => {
                    debug!("Debug stop: {:?}", info);
                    dump_regs(&self.vcpu);
                }
                r => {
                    debug!("unexpected exit reason: {:?}", r);
                    dump_regs(&self.vcpu);
                    break;
                }
            }
        }

        // get iterator over the page table after the function ran
        let page_iter = PageMapeIterator::new(kvm_context.backing_memory.get_mut_u64(), &self.vcpu);
        for (virtual_address, owned, physical) in page_iter {
            debug!(
                "Found page for virtual {}, owned: {}, at {}",
                virtual_address, owned, physical
            );
            // look up if the overlay already has an entry
            match kvm_context.overlay.entry(virtual_address) {
                // if the overlay did not have an entry before, this is a newly allocated page that we need to take ownership of
                Entry::Vacant(vacant) => {
                    vacant.insert((
                        PageType::Page,
                        Page::new(physical, kvm_context.backing_memory.clone()),
                    ));
                }
                // if the overlay had an entry, check if the ownership still matches, if not overwrite the entry
                Entry::Occupied(mut occupied) => {
                    let previous_entry = occupied.get();
                    match previous_entry.0 {
                        // if previously was a copy on write page and now we have an owned one, overwrite it
                        PageType::CoW if owned => {
                            occupied.insert((
                                PageType::Page,
                                Page::new(physical, kvm_context.backing_memory.clone()),
                            ));
                        }
                        // otherwise is cow and still not owned or was alreay owned, sanity check that it has stil same address
                        _ => debug_assert_eq!(*previous_entry.1, physical),
                    }
                }
            }
        }

        read_output_structs::<u64, u64>(&mut context, elf_config.system_data_offset)?;

        // TODO: after merge, create items that only contain the pages for that item

        Ok(context)
    }
}

pub struct KvmDriver {}

impl Driver for KvmDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: impl EngineWorkQueue + Send + 'static,
    ) -> DandelionResult<()> {
        let cpu_slot = match resource {
            ComputeResource::CPU(core) => core,
            _ => return err_dandelion!(DandelionError::EngineResourceError),
        };
        let available_cores = match core_affinity::get_core_ids() {
            None => return err_dandelion!(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(cpu_slot))
            .is_some()
        {
            return err_dandelion!(DandelionError::EngineResourceError);
        }
        start_thread::<KvmLoop>(cpu_slot, queue);
        Ok(())
    }

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &dyn MemoryDomain,
    ) -> DandelionResult<Function> {
        let function = load_u8_from_file(function_path)?;
        let elf = elf_parser::ParsedElf::new(&function)?;
        let system_data = elf.get_symbol_by_name(&function, "__dandelion_system_data")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(ElfConfig {
            system_data_offset: system_data.0,
            entry_point: entry,
        });
        let (static_requirements, source_layout) = elf.get_layout_pair();

        // place the code at the place it will be in the final context, so we can remap
        let size = static_requirements
            .iter()
            .map(|pos| pos.offset + pos.size)
            .max()
            .unwrap_or_default();

        let mut context = static_domain.acquire_context(size)?;
        // copy all
        let mut new_content = DataSet {
            ident: String::from("static"),
            buffers: vec![],
        };
        let buffers = &mut new_content.buffers;
        for (required_position, source_position) in
            static_requirements.iter().zip(source_layout.iter())
        {
            context.write(
                required_position.offset,
                &function[source_position.offset..source_position.offset + source_position.size],
            )?;
            // make sure to write 0s to fill in for the difference between the source and required position
            if source_position.size < required_position.size {
                // TODO there should be a better way to do this
                let zeros = vec![0u8; required_position.size - source_position.size];
                context.write(required_position.offset + source_position.size, &zeros)?
            }
            buffers.push(DataItem {
                ident: String::from(""),
                data: Position {
                    offset: required_position.offset,
                    size: required_position.size,
                },
                key: 0,
            });
        }
        context.content = vec![Some(new_content)];

        let requirements = DataRequirementList {
            input_requirements: Vec::<DataRequirement>::new(),
            static_requirements,
        };

        Ok(Function {
            requirements,
            context: Arc::new(context),
            config,
        })
    }
}
