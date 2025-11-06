use crate::{
    function_driver::{
        load_utils::load_u8_from_file,
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, ElfConfig, Function, FunctionConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, DataSet, Position,
};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult};
use kvm_bindings::{kvm_userspace_memory_region, KVM_MAX_CPUID_ENTRIES};
use kvm_ioctls::{Kvm, VcpuExit, VcpuFd, VmFd};
use log::debug;
use nix::sys::mman::{mmap, MapFlags, ProtFlags};
use std::{num::NonZeroUsize, sync::Arc};

#[cfg(target_arch = "x86_64")]
mod x86_64;
#[cfg(target_arch = "x86_64")]
pub use x86_64::PAGE_SIZE;
#[cfg(target_arch = "x86_64")]
use x86_64::*;

#[cfg(target_arch = "aarch64")]
mod aarch64;
#[cfg(target_arch = "aarch64")]
use aarch64::*;

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
        let mut mappings = Vec::new();
        let mut removed_overlay = Vec::new();
        // go through things that are overlayed and map, it was made sure in the transfer function, that it is full pages
        // TODO: when cursor is stabilized use that, so mappings can be removed if they were copied
        for (&overlay_end, (overlay_start, overlay_context)) in kvm_context.overlay.iter() {
            // map from back if it is a kvm context
            if let ContextType::Kvm(overlay_kvm_context) = &overlay_context.context.context {
                let overlay_size = overlay_end - *overlay_start + 1;
                // map to end of context
                let mut mappig_start = stack_start - overlay_size;
                // make sure that the virtual and physical address have the same allignment with regards to large pages
                // for this mapping start needs to have the same distance to the next large page boundry as the virtual
                let virtual_large_offset =
                    overlay_start.next_multiple_of(LARGE_PAGE) - *overlay_start;
                let mapping_large_offset = mappig_start.next_multiple_of(LARGE_PAGE) - mappig_start;
                let additional_offset = if virtual_large_offset >= mapping_large_offset {
                    virtual_large_offset - mapping_large_offset
                } else {
                    virtual_large_offset + LARGE_PAGE - mapping_large_offset
                };
                mappig_start -= additional_offset;
                stack_start = mappig_start;
                let start_address = kvm_context.storage.as_ptr().addr() + stack_start;
                let file_offset = (overlay_kvm_context.rangepool_start as usize) * PAGE_SIZE
                    + overlay_context.offset;
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
                mappings.push((*overlay_start, mappig_start, overlay_size));

                log::debug!(
                    "zero copy pages at physical: {}, virtual {}, with size {}",
                    mappig_start,
                    *overlay_start,
                    overlay_size
                );
            } else {
                let overlay_size = overlay_end - *overlay_start + 1;
                let mut read_bytes = 0;
                while read_bytes < overlay_size {
                    let chunk = overlay_context.context.get_chunk_ref(
                        overlay_context.offset + read_bytes,
                        overlay_size - read_bytes,
                    )?;
                    kvm_context.storage
                        [*overlay_start + read_bytes..*overlay_start + read_bytes + chunk.len()]
                        .copy_from_slice(chunk);
                    read_bytes += chunk.len();
                }
                removed_overlay.push(overlay_end);

                log::debug!(
                    "manually copied overlayed context into virtual {} with size {}",
                    *overlay_start,
                    overlay_size
                );
            }
        }
        for key in removed_overlay {
            kvm_context.overlay.remove(&key);
        }

        // attach VM memory
        let mut region = kvm_userspace_memory_region {
            slot: 0,
            flags: 0,
            guest_phys_addr: 0x0,
            memory_size: kvm_context.storage.len() as u64,
            userspace_addr: kvm_context.storage.as_ptr() as u64,
        };
        unsafe {
            self.vm.set_user_memory_region(region).unwrap();
        }

        // initialize vCPU
        self.state.init_vcpu(
            &self.vcpu,
            elf_config.entry_point as u64,
            kvm_context.storage,
            mappings,
            stack_start,
            kvm_context.storage.len(),
        );

        #[cfg(feature = "backend_debug")]
        {
            // configure single-step debugging
            step_debug(&self.vcpu);
            dump_regs(&self.vcpu);
        }

        let mut copied_pages = Vec::new();
        // start running the function
        loop {
            let reason = self.vcpu.run().unwrap();
            match reason {
                VcpuExit::IoOut(14, _) => {
                    // handle page fault in guest
                    let page_offset = handle_page_fault(&self.vcpu, kvm_context.storage)?;
                    copied_pages.push(page_offset);
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

        // detach VM memory
        region.memory_size = 0;
        unsafe {
            self.vm.set_user_memory_region(region).unwrap();
        }

        // fix context overlay
        copied_pages.sort();

        // punching holes in the overlay, since there are only holes to be punched
        // where there was overlay, panic if there is no overlap
        for start in copied_pages {
            // TODO: use cursor once it stabilizes
            let (to_insert_opt, to_remove_opt) =
                if let Some((&overlay_end, (overlay_start, overlay_item))) =
                    kvm_context.overlay.range_mut(start..).next()
                {
                    // know that start < overlay_end, so for overlap need to check that start is not too early
                    if start < *overlay_start {
                        panic!(
                            "Trying to punch hole at {} in overlay that starts only later {}",
                            start, overlay_start
                        );
                    // page is at start of overlay, so can just shrink it
                    } else if start == *overlay_start {
                        *overlay_start += PAGE_SIZE;
                        if *overlay_start > overlay_end {
                            (None, Some(overlay_end))
                        } else {
                            (None, None)
                        }
                    // page is in middle, so need to cut it in two
                    } else {
                        let new_overlay = (start - 1, (*overlay_start, overlay_item.clone()));
                        *overlay_start = start + PAGE_SIZE;
                        overlay_item.offset += start + PAGE_SIZE - *overlay_start;
                        if *overlay_start > overlay_end {
                            (Some(new_overlay), Some(overlay_end))
                        } else {
                            (Some(new_overlay), None)
                        }
                    }
                } else {
                    // there is no overlay ending after the current one starting, which means something went wrong.
                    panic!(
                        "trying to punch hole in non existent overlay: page {}",
                        start
                    );
                };
            if let Some(end) = to_remove_opt {
                kvm_context.overlay.remove(&end);
            }
            if let Some((key, value)) = to_insert_opt {
                kvm_context.overlay.insert(key, value);
            }
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
