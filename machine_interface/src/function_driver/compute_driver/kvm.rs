use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, ElfConfig, Function, FunctionConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs, write_heap_end},
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult, UserError};
use kvm_bindings::{kvm_userspace_memory_region, KVM_MAX_CPUID_ENTRIES, KVM_MEM_LOG_DIRTY_PAGES};
use kvm_ioctls::{Kvm, VcpuExit, VcpuFd, VmFd};
use log::debug;
use nix::sys::mman::{mmap, MapFlags, ProtFlags};
use std::{num::NonZeroUsize, sync::Arc};

#[cfg(target_arch = "x86_64")]
mod x86_64;
#[cfg(target_arch = "x86_64")]
mod x86_64_asm;
#[cfg(target_arch = "x86_64")]
pub use x86_64::PAGE_SIZE;
#[cfg(target_arch = "x86_64")]
pub(self) use x86_64::*;

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
        // enable all features the real cpu has on the vcpu
        let cpuid = kvm.get_supported_cpuid(KVM_MAX_CPUID_ENTRIES).unwrap();
        vcpu.set_cpuid2(&cpuid).unwrap();

        let state = ResetState::new(&vm, &vcpu);

        return Ok(Box::new(KvmLoop { vm, vcpu, state }));
    }

    fn run(
        &mut self,
        config: FunctionConfig,
        mut context: Context,
        output_sets: Arc<Vec<String>>,
    ) -> DandelionResult<Context> {
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Err(DandelionError::ConfigMissmatch),
        };
        setup_input_structs::<u64, u64>(&mut context, elf_config.system_data_offset, &output_sets)?;
        let min_stack_start = context.get_last_item_end();
        let kvm_context = match &mut context.context {
            ContextType::Kvm(kvm_context) => kvm_context,
            _ => return Err(DandelionError::ContextMissmatch),
        };

        #[cfg(feature = "backend_debug")]
        {
            println!("context ptr: {:?}", kvm_context.storage.as_ptr());
            println!("context size: {}", kvm_context.storage.len());
            println!("entry point: {:#x}", elf_config.entry_point);
            dump_memory(&kvm_context.storage[elf_config.entry_point..elf_config.entry_point + 64]);
        }

        let mut stack_start = kvm_context.storage.len();
        // vector containing the mapping where something should be, where it
        let mut mappings = Vec::with_capacity(kvm_context.overlay.len());
        // go through things that are overlayed and map, it was made sure in the transfer function, that it is full pages
        // TODO: when cursor is stabilized use that, so mappings can be removed if they were copied
        for (&overlay_end, (overlay_start, overlay_context)) in kvm_context.overlay.iter_mut() {
            // map from back if it is a kvm context
            let original = if let Some(context_item) = overlay_context {
                if let ContextType::Kvm(overlay_kvm_context) = &context_item.context.context {
                    let overlay_size = overlay_end - *overlay_start + 1;
                    // map to end of context
                    let mut mappig_start = stack_start - overlay_size;
                    // make sure that the virtual and physical address have the same allignment with regards to large pages
                    // for this mapping start needs to have the same distance to the next large page boundry as the virtual
                    let virtual_large_offset =
                        overlay_start.next_multiple_of(LARGE_PAGE) - *overlay_start;
                    let mapping_large_offset =
                        mappig_start.next_multiple_of(LARGE_PAGE) - mappig_start;
                    let additional_offset = if virtual_large_offset >= mapping_large_offset {
                        virtual_large_offset - mapping_large_offset
                    } else {
                        virtual_large_offset + LARGE_PAGE - mapping_large_offset
                    };
                    mappig_start -= additional_offset;
                    stack_start = mappig_start;
                    let start_address = kvm_context.storage.as_ptr().addr() + stack_start;
                    let file_offset = (overlay_kvm_context.rangepool_start as usize) * PAGE_SIZE
                        + context_item.offset;
                    unsafe {
                        mmap(
                            NonZeroUsize::new(start_address),
                            NonZeroUsize::new_unchecked(overlay_size),
                            ProtFlags::all(),
                            MapFlags::MAP_PRIVATE | MapFlags::MAP_FIXED,
                            overlay_kvm_context.fd,
                            file_offset as i64,
                        )
                        .unwrap()
                    };

                    log::debug!(
                        "zero copy pages at physical: {}, virtual {}, with size {}",
                        mappig_start,
                        *overlay_start,
                        overlay_size
                    );
                    Some(mappig_start)
                } else {
                    panic!("KVM context overlay should not contain context reference that is not remappable");
                }
            } else {
                None
            };
            // push each mapping into the vec
            mappings.push((*overlay_start, overlay_end, original));
        }

        // attach VM memory
        let mut region = kvm_userspace_memory_region {
            slot: 0,
            flags: KVM_MEM_LOG_DIRTY_PAGES,
            guest_phys_addr: 0x0,
            memory_size: kvm_context.storage.len() as u64,
            userspace_addr: kvm_context.storage.as_ptr() as u64,
        };
        unsafe {
            self.vm.set_user_memory_region(region).unwrap();
        }

        // initialize vCPU
        let page_fault_metadata = self.state.init_vcpu(
            &self.vcpu,
            elf_config.entry_point as u64,
            kvm_context.storage,
            mappings,
            stack_start,
            kvm_context.storage.len(),
        )?;

        // make sure that the stack start has not moved into the occupied territory
        stack_start = page_fault_metadata.get_stack_start();
        write_heap_end::<u64, u64>(
            kvm_context,
            elf_config.system_data_offset,
            stack_start as u64,
        )?;
        if min_stack_start >= stack_start {
            return Err(DandelionError::UserError(UserError::SmallContext));
        }

        #[cfg(feature = "backend_debug")]
        {
            // configure single-step debugging
            step_debug(&self.vcpu);
            dump_regs(&self.vcpu);
        }

        // start running the function
        loop {
            let reason = self.vcpu.run().unwrap();
            match reason {
                #[cfg(feature = "backend_debug")]
                VcpuExit::IoOut(14, _) => {
                    // ATTENTION: unomment out 14 in page handler so it exits to here
                    // handle page fault in guest
                    check_page_fault_handling(
                        &self.vcpu,
                        &page_fault_metadata,
                        kvm_context.storage,
                    );
                }
                VcpuExit::Hlt => break,
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

        let dirty_log = self.vm.get_dirty_log(0, kvm_context.storage.len()).unwrap();

        // detach VM memory
        region.memory_size = 0;
        unsafe {
            self.vm.set_user_memory_region(region).unwrap();
        }

        let mut dirty_index = 0;
        let mut contiguous_pages = 0;
        while dirty_index < dirty_log.len() {
            let mut local_dirty = dirty_log[dirty_index];
            if local_dirty == 0 {
                if contiguous_pages > 0 {
                    let end = dirty_index * 64 * PAGE_SIZE;
                    let start = end - contiguous_pages * PAGE_SIZE;
                    kvm_context.insert_into_overlay(start, end, None);
                    contiguous_pages = 0;
                }
            } else if local_dirty == u64::MAX {
                contiguous_pages += 64;
            } else {
                let mut bits_processed = 0usize;
                let mut trailing_zeros = local_dirty.trailing_zeros() as usize;
                if trailing_zeros != 0 && contiguous_pages != 0 {
                    let end = dirty_index * 64 * PAGE_SIZE;
                    let start = end - contiguous_pages * PAGE_SIZE;
                    kvm_context.insert_into_overlay(start, end, None);
                    contiguous_pages = 0;
                }
                while trailing_zeros < 64 {
                    // can always do this, sice if the last one is not a zero it will simply shift by 0
                    local_dirty = local_dirty >> trailing_zeros;
                    bits_processed += trailing_zeros;
                    let trailing_ones = local_dirty.trailing_ones() as usize;
                    contiguous_pages += trailing_ones;
                    local_dirty = local_dirty >> trailing_ones;
                    bits_processed += trailing_ones;
                    // if the trailing ones were until the end of the u64, break and continue with the next u64
                    if bits_processed >= 64 {
                        break;
                    }
                    let end = (dirty_index * 64 + bits_processed) * PAGE_SIZE;
                    let start = end - contiguous_pages * PAGE_SIZE;
                    kvm_context.insert_into_overlay(start, end, None);
                    contiguous_pages = 0;
                    trailing_zeros = local_dirty.trailing_zeros() as usize;
                }
            }
            dirty_index += 1;
        }
        if contiguous_pages > 0 {
            let end = dirty_index * 64 * PAGE_SIZE;
            let start = end - contiguous_pages * PAGE_SIZE;
            kvm_context.insert_into_overlay(start, end, None);
        }

        read_output_structs::<u64, u64>(&mut context, elf_config.system_data_offset)?;
        return Ok(context);
    }
}

pub struct KvmDriver {}

impl Driver for KvmDriver {
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send>,
    ) -> DandelionResult<()> {
        let cpu_slot = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(DandelionError::EngineError),
            Some(cores) => cores,
        };
        if !available_cores
            .iter()
            .find(|x| x.id == usize::from(cpu_slot))
            .is_some()
        {
            return Err(DandelionError::EngineResourceError);
        }
        start_thread::<KvmLoop>(cpu_slot, queue);
        return Ok(());
    }

    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        &self,
        function_path: String,
        static_domain: &Box<dyn MemoryDomain>,
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
            static_requirements: static_requirements,
        };

        return Ok(Function {
            requirements,
            context: Arc::new(context),
            config,
        });
    }
}
