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
use log::{debug, warn};
use nix::{
    sys::{
        signal::Signal,
        wait::{self, WaitStatus},
    },
    unistd::Pid,
};
use std::{
    process::{Command, Stdio},
    sync::Arc,
};

fn ptrace_syscall(pid: libc::pid_t) {
    #[cfg(target_os = "linux")]
    let res = unsafe { libc::ptrace(libc::PTRACE_SYSCALL, pid, 0, 0) };
    #[cfg(target_os = "freebsd")]
    let res = unsafe { libc::ptrace(libc::PT_SYSCALL, pid, 1 as *mut _, 0) };
    assert_eq!(res, 0);
}

enum SyscallType {
    Exit,
    #[cfg(target_arch = "x86_64")]
    Authorized,
    Unauthorized(i64),
}

#[cfg(target_os = "linux")]
fn check_syscall(pid: libc::pid_t) -> SyscallType {
    type Regs = libc::user_regs_struct;
    let regs = unsafe {
        let mut regs_uninit: core::mem::MaybeUninit<Regs> = core::mem::MaybeUninit::uninit();
        let io = libc::iovec {
            iov_base: regs_uninit.as_mut_ptr() as *mut _,
            iov_len: core::mem::size_of::<Regs>(),
        };
        let res = libc::ptrace(libc::PTRACE_GETREGSET, pid, libc::NT_PRSTATUS, &io);
        assert_eq!(res, 0);
        regs_uninit.assume_init()
    };
    #[cfg(target_arch = "x86_64")]
    let syscall_id = regs.orig_rax as i64;
    #[cfg(target_arch = "aarch64")]
    let syscall_id = regs.regs[8] as i64;
    match syscall_id {
        libc::SYS_exit | libc::SYS_exit_group => SyscallType::Exit,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_arch_prctl => SyscallType::Authorized, // TODO: check arguments
        id => SyscallType::Unauthorized(id),
    }
}

#[cfg(target_os = "freebsd")]
fn check_syscall(pid: libc::pid_t) -> SyscallType {
    #[cfg(target_arch = "x86_64")]
    type Regs = libc::reg;
    #[cfg(target_arch = "aarch64")]
    type Regs = libc::gpregs;
    let regs = unsafe {
        let mut regs_uninit: core::mem::MaybeUninit<Regs> = core::mem::MaybeUninit::uninit();
        let res = libc::ptrace(libc::PT_GETREGS, pid, regs_uninit.as_mut_ptr() as *mut _, 0);
        assert_eq!(res, 0);
        regs_uninit.assume_init()
    };
    #[cfg(target_arch = "x86_64")]
    let syscall_id = regs.r_rax;
    #[cfg(target_arch = "aarch64")]
    let syscall_id = regs.gp_x[0];
    match syscall_id {
        1 => SyscallType::Exit,
        id => SyscallType::Unauthorized(id),
    }
}

fn mmu_run_static(
    cpu_slot: u8,
    storage_id: &str,
    protection_flags: &[(u32, Position)],
    entry_point: usize,
) -> DandelionResult<()> {
    // TODO: modify ELF header
    // to load mmu_worker into a safe address range
    // that will not collide with those used by user's function

    // this trick gives the desired path of mmu_worker for packages within the workspace
    let path = std::env::var("PROCESS_WORKER_PATH").unwrap_or(format!(
        "{}/../target/{}-unknown-linux-gnu/{}/mmu_worker",
        env!("CARGO_MANIFEST_DIR"),
        std::env::consts::ARCH,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
    ));

    // create a new address space (child process) and pass the shared memory
    let mut worker = Command::new(path)
        .arg(cpu_slot.to_string())
        .arg(storage_id)
        .arg(entry_point.to_string())
        .arg(serde_json::to_string(protection_flags).unwrap())
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|_e| DandelionError::MmuWorkerError)?;
    debug!("created a new process");

    // intercept worker's syscalls by ptrace
    let pid = Pid::from_raw(worker.id() as i32);
    let status = wait::waitpid(pid, None).map_err(|_e| DandelionError::MmuWorkerError)?;
    if status != WaitStatus::Stopped(pid, Signal::SIGSTOP) {
        worker.kill().map_err(|_e| DandelionError::MmuWorkerError)?;
    }
    ptrace_syscall(pid.as_raw());

    loop {
        let status = wait::waitpid(pid, None).map_err(|_e| DandelionError::MmuWorkerError)?;
        let WaitStatus::Stopped(pid, sig) = status else {
            panic!("worker should be stopped (status = {:?})", status);
        };
        match sig {
            Signal::SIGTRAP => match check_syscall(pid.as_raw()) {
                SyscallType::Exit => {
                    debug!("detected exit syscall");
                    ptrace_syscall(pid.as_raw());
                    let status = worker.wait().map_err(|_e| DandelionError::MmuWorkerError)?;
                    debug!("worker exited with code {}", status.code().unwrap());
                    return Ok(());
                }
                #[cfg(target_arch = "x86_64")]
                SyscallType::Authorized => {
                    debug!("detected authorized syscall");
                    ptrace_syscall(pid.as_raw());
                }
                SyscallType::Unauthorized(syscall_id) => {
                    warn!("detected unauthorized syscall with id {}", syscall_id);
                    worker.kill().map_err(|_e| DandelionError::MmuWorkerError)?;
                    warn!("worker killed");
                    return Err(DandelionError::UnauthorizedSyscall);
                }
            },
            Signal::SIGSEGV => {
                warn!("detected segmentation fault");
                return Err(DandelionError::SegmentationFault);
            }
            s => {
                warn!("detected {}", s);
                return Err(DandelionError::OtherProctionError);
            }
        }
    }
}

struct MmuLoop {
    cpu_slot: u8,
}

impl EngineLoop for MmuLoop {
    fn init(core_id: u8) -> DandelionResult<Box<Self>> {
        return Ok(Box::new(MmuLoop { cpu_slot: core_id }));
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

        setup_input_structs::<usize, usize>(
            &mut context,
            elf_config.system_data_offset,
            &output_sets,
        )?;

        let mmu_context = match &context.context {
            ContextType::Mmu(mmu_context) => mmu_context,
            _ => return Err(DandelionError::ContextMissmatch),
        };

        mmu_run_static(
            self.cpu_slot,
            mmu_context.storage.filename().unwrap(),
            &elf_config.protection_flags,
            elf_config.entry_point,
        )?;

        read_output_structs::<usize, usize>(&mut context, elf_config.system_data_offset)?;

        return Ok(context);
    }
}

pub struct MmuDriver {}

impl Driver for MmuDriver {
    // // take or release one of the available engines
    fn start_engine(
        &self,
        resource: ComputeResource,
        queue: Box<dyn WorkQueue + Send + Sync>,
    ) -> DandelionResult<()> {
        let cpu_slot = match resource {
            ComputeResource::CPU(core) => core,
            _ => return Err(DandelionError::EngineResourceError),
        };
        // check that core is available
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
        start_thread::<MmuLoop>(cpu_slot, queue);
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
        //let return_offset = elf.get_symbol_by_name(&function, "__dandelion_return_address")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(ElfConfig {
            system_data_offset: system_data.0,
            #[cfg(feature = "cheri")]
            return_offset: (0, 0),
            entry_point: entry,
            protection_flags: Arc::new(elf.get_memory_protection_layout()),
        });
        let (static_requirements, source_layout) = elf.get_layout_pair();
        let requirements = DataRequirementList {
            input_requirements: Vec::<DataRequirement>::new(),
            static_requirements: static_requirements,
        };
        // sum up all sizes
        let mut total_size = 0;
        for position in source_layout.iter() {
            total_size += position.size;
        }
        let mut context = static_domain.acquire_context(total_size)?;
        // copy all
        let mut write_counter = 0;
        let mut new_content = DataSet {
            ident: String::from("static"),
            buffers: vec![],
        };
        let buffers = &mut new_content.buffers;
        for position in source_layout.iter() {
            context.write(
                write_counter,
                &function[position.offset..position.offset + position.size],
            )?;
            buffers.push(DataItem {
                ident: String::from(""),
                data: Position {
                    offset: write_counter,
                    size: position.size,
                },
                key: 0,
            });
            write_counter += position.size;
        }
        context.content = vec![Some(new_content)];
        return Ok(Function {
            requirements,
            context,
            config,
        });
    }
}

#[cfg(test)]
mod test;
