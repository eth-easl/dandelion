use crate::{
    function_driver::{
        thread_utils::{start_thread, EngineLoop},
        ComputeResource, Driver, ElfConfig, Function, FunctionConfig, WorkQueue,
    },
    interface::{read_output_structs, setup_input_structs},
    memory_domain::{Context, ContextType},
    util::elf_parser,
    DataRequirementList,
};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult};
use kvm_bindings::kvm_userspace_memory_region;
use kvm_ioctls::{Kvm, VcpuExit, VcpuFd, VmFd};
use std::sync::Arc;

#[cfg(target_arch = "x86_64")]
mod x86_64;
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
            ContextType::Mmap(mmap_context) => mmap_context,
            _ => return Err(DandelionError::ContextMissmatch),
        };
        let guest_mem = unsafe { kvm_context.storage.as_slice_mut() };
        #[cfg(feature = "backend_debug")]
        {
            println!("context ptr: {:?}", guest_mem.as_ptr());
            println!("context size: {}", guest_mem.len());
            println!("entry point: {:#x}", elf_config.entry_point);
            dump_memory(&guest_mem[elf_config.entry_point..elf_config.entry_point + 64]);
        }

        // attach VM memory
        let mut region = kvm_userspace_memory_region {
            slot: 0,
            flags: 0,
            guest_phys_addr: 0x0,
            memory_size: guest_mem.len() as u64,
            userspace_addr: guest_mem.as_ptr() as u64,
        };
        unsafe {
            self.vm.set_user_memory_region(region).unwrap();
        }

        // initialize vCPU
        self.state.init_vcpu(
            &self.vcpu,
            elf_config.entry_point as u64,
            guest_mem.len() as u64 - 32,
        );
        self.state.set_page_table(guest_mem);

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
                VcpuExit::Hlt => break,
                VcpuExit::SystemEvent(_type, _data) => break,
                VcpuExit::Debug(info) => {
                    println!("Debug stop: {:?}", info);
                    dump_regs(&self.vcpu);
                }
                r => {
                    println!("unexpected exit reason: {:?}", r);
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
    fn parse_function(&self, function: &Vec<u8>) -> DandelionResult<Function> {
        let elf = elf_parser::ParsedElf::new(function)?;
        let system_data = elf.get_symbol_by_name(&function, "__dandelion_system_data")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(ElfConfig {
            system_data_offset: system_data.0,
            entry_point: entry,
        });
        let mut layout = elf.get_layout_pair();

        // sum up all sizes
        let mut total_size = 0;
        for (_, position) in layout.iter() {
            total_size += position.size;
        }
        let mut static_data = Vec::with_capacity(total_size);
        for (_, position) in layout.iter_mut() {
            let binary_start = static_data.len();
            let elf_start = position.offset;
            let elf_end = elf_start + position.size;
            static_data.extend_from_slice(&function[elf_start..elf_end]);
            position.offset = binary_start;
        }

        return Ok(Function {
            requirements: DataRequirementList {
                input_requirements: Vec::new(),
                static_requirements: layout,
            },
            static_data,
            config,
        });
    }
}
