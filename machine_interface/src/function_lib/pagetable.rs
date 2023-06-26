use crate::{
    function_lib::{Driver, ElfConfig, Engine, FunctionConfig, Loader},
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataRequirement, DataRequirementList, Position,
};
use core::{future::ready, pin::Pin};
use core_affinity;
use dandelion_commons::{DandelionError, DandelionResult};
use nix::{
    sys::{
        signal::Signal,
        wait::{self, WaitStatus},
    },
    unistd::Pid,
};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
};

use super::Function;

const IO_STRUCT_SIZE: usize = 16;
const MAX_OUTPUTS: u32 = 16;

fn setup_input_structs(
    context: &mut Context,
    config: &ElfConfig,
    base_addr: usize,
) -> DandelionResult<()> {
    // size of array with input struct array
    let input_number_option = context.dynamic_data.keys().max();
    let input_number = match input_number_option {
        Some(num) => num + 1,
        None => return Ok(()),
    };
    let input_struct_size = input_number as usize * IO_STRUCT_SIZE;
    let input_offset = context.get_free_space(input_struct_size, 8)?;
    for (index, input) in context.dynamic_data.iter() {
        let pos = match &input {
            DataItem::Item(position) => position,
            _ => return Err(DandelionError::NotImplemented),
        };
        let struct_offset = input_offset + IO_STRUCT_SIZE * index;
        let mut io_struct = Vec::<u8>::new();
        io_struct.append(&mut usize::to_ne_bytes(pos.size).to_vec());
        io_struct.append(&mut usize::to_ne_bytes(base_addr + pos.offset).to_vec());
        context.context.write(struct_offset, io_struct)?;
    }
    context.static_data.push(Position {
        offset: input_offset,
        size: input_struct_size,
    });
    // find space for output structs
    let output_struct_size = IO_STRUCT_SIZE * MAX_OUTPUTS as usize;
    let output_offset = context.get_free_space(output_struct_size, 8)?;
    context.static_data.push(Position {
        offset: output_offset,
        size: output_struct_size,
    });
    // fill in values
    context.write(
        config.input_root.0,
        usize::to_ne_bytes(base_addr + input_offset).to_vec(),
    )?;
    context.write(
        config.input_number.0,
        u32::to_ne_bytes(input_number as u32).to_vec(),
    )?;
    context.write(
        config.output_root.0,
        usize::to_ne_bytes(base_addr + output_offset).to_vec(),
    )?;
    context.write(config.output_number.0, u32::to_ne_bytes(0).to_vec())?;
    context.write(
        config.max_output_number.0,
        u32::to_ne_bytes(MAX_OUTPUTS).to_vec(),
    )?;
    return Ok(());
}

fn get_output_layout(
    context: &mut Context,
    config: &ElfConfig,
    base_addr: usize,
) -> DandelionResult<()> {
    // get output number
    let output_number_vec = context.read(config.output_number.0, config.output_number.1)?;
    // TODO make this dependent on the actual size of the values
    // TODO use as_chunks when it stabilizes
    let output_number_slice: [u8; 4] = output_number_vec[0..4]
        .try_into()
        .expect("Should have correct length");
    let number_of_outputs = usize::min(
        u32::from_ne_bytes(output_number_slice) as usize,
        MAX_OUTPUTS as usize,
    );
    let mut output_structs = HashMap::new();
    let output_root_vec = context.read(config.output_root.0, 8)?;
    let output_root_offset = usize::from_ne_bytes(
        output_root_vec[0..8]
            .try_into()
            .expect("Should have correct length"),
    );
    for output_index in 0..number_of_outputs {
        // let read_offset = output_root_offset + IO_STRUCT_SIZE * output_index;
        let read_offset = output_root_offset + IO_STRUCT_SIZE * output_index - base_addr;
        let out_struct = context.read(read_offset, IO_STRUCT_SIZE as usize)?;
        let size = usize::from_ne_bytes(
            out_struct[0..8]
                .try_into()
                .expect("Should have correct length"),
        );
        let offset = usize::from_ne_bytes(
            out_struct[8..16]
                .try_into()
                .expect("Should have correct length"),
        );
        output_structs.insert(
            output_index,
            DataItem::Item(Position {
                offset: offset - base_addr as usize,
                size: size,
            }),
        );
    }
    context.dynamic_data = output_structs;
    Ok(())
}

// struct CheriCommand {
//     context: *const cheri_c_context,
//     entry_point: size_t,
//     return_pair_offset: size_t,
//     stack_pointer: size_t,
// }
// unsafe impl Send for CheriCommand {}

pub struct PagetableEngine {
    is_running: AtomicBool,
    cpu_slot: u8,
    // command_sender: Sender<CheriCommand>,
    // result_receiver: Receiver<DandelionResult<()>>,
    // thread_handle: JoinHandle<()>,
}

// fn run_thread(
//     core_id: u8,
//     // command_receiver: Receiver<CheriCommand>,
//     result_sender: Sender<DandelionResult<()>>,
// ) -> () {
//     // set core
//     if !core_affinity::set_for_current(core_affinity::CoreId { id: core_id.into() }) {
//         return;
//     };
//     for command in command_receiver.iter() {
//         let cheri_error;
//         unsafe {
//             cheri_error = cheri_run_static(
//                 command.context,
//                 command.entry_point,
//                 command.return_pair_offset,
//                 command.stack_pointer,
//             );
//         }
//         let message = match cheri_error {
//             0 => Ok(()),
//             1 => Err(DandelionError::OutOfMemory),
//             _ => Err(DandelionError::NotImplemented),
//         };
//         if result_sender.send(message).is_err() {
//             return;
//         }
//     }
// }

fn ptrace_syscall(pid: libc::pid_t) {
    #[cfg(target_os = "linux")]
    let res = unsafe { libc::ptrace(libc::PTRACE_SYSCALL, pid, 0, 0) };
    #[cfg(target_os = "freebsd")]
    let res = unsafe { libc::ptrace(libc::PT_SYSCALL, pid, 1 as *mut _, 0) };
    assert_eq!(res, 0);
}

enum SyscallType {
    Exit,
    Authorized,
    Unauthorized(i64),
}

#[cfg(target_os = "linux")]
fn check_syscall(pid: libc::pid_t) -> SyscallType {
    type Regs = libc::user_regs_struct;
    let regs = unsafe {
        let mut regs_uninit: core::mem::MaybeUninit<Regs> = core::mem::MaybeUninit::uninit();
        // TODO PTRACE_GETREGS seems to be missing on linux;
        // is this the correct replacement?
        let res: i64 = libc::ptrace(libc::PTRACE_GETREGSET, pid, 0, regs_uninit.as_mut_ptr());
        assert_eq!(res, 0);
        regs_uninit.assume_init()
    };
    #[cfg(target_arch = "x86_64")]
    let syscall_id = regs.orig_rax as i64;
    #[cfg(target_arch = "aarch64")]
    let syscall_id = regs.regs[0] as i64;
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

impl Engine for PagetableEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
    ) -> Pin<Box<dyn futures::Future<Output = (DandelionResult<()>, Context)> + '_ + Send>> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Box::pin(ready((Err(DandelionError::EngineAlreadyRunning), context)));
        }
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Box::pin(ready((Err(DandelionError::ConfigMissmatch), context))),
        };
        let pagetable_context = match &context.context {
            ContextType::Pagetable(pagetable_context) => pagetable_context,
            _ => return Box::pin(ready((Err(DandelionError::ContextMissmatch), context))),
        };
        // let command = CheriCommand {
        //     context: cheri_context.context,
        //     entry_point: elf_config.entry_point,
        //     return_pair_offset: elf_config.return_offset.0,
        //     stack_pointer: cheri_context.size,
        // };

        // let storage = pagetable_context.storage.as_ptr();
        // let storage_len = pagetable_context.storage.len();
        // match self.command_sender.send(command) {
        //     Err(_) => return (Err(DandelionError::EngineError), context),
        //     Ok(_) => (),
        // }
        // match self.result_receiver.recv() {
        //     Err(_) => return (Err(DandelionError::EngineError), context),
        //     Ok(Err(err)) => return (Err(err), context),
        //     Ok(Ok(())) => (),
        // }

        // create a new address space (child process) and pass the shared memory
        let mut worker = Command::new("../target/debug/pagetable_worker")
            .arg(self.cpu_slot.to_string())
            .arg(pagetable_context.storage.id())
            .arg(serde_json::to_string(&context.protection_requirements).unwrap())
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        eprintln!("created a new process");

        // receive the shared memory address in worker's address space
        let mut reader = BufReader::new(worker.stdout.take().unwrap());
        let mut buf: String = String::new();
        reader.read_line(&mut buf).unwrap();
        let worker_base_addr: usize = buf.trim().parse().unwrap();
        eprintln!("got base address {:x}", worker_base_addr);

        if let Err(err) = setup_input_structs(&mut context, &elf_config, worker_base_addr) {
            return Box::pin(ready((Err(err), context)));
        }

        // send the entry point of user's code to worker
        worker
            .stdin
            .take()
            .unwrap()
            .write_all(elf_config.entry_point.to_string().as_bytes())
            .unwrap();
        eprintln!("sent entry point");

        // intercept worker's syscalls by ptrace
        let pid = Pid::from_raw(worker.id() as i32);
        let status = wait::waitpid(pid, None).unwrap();
        assert_eq!(status, WaitStatus::Stopped(pid, Signal::SIGSTOP));
        ptrace_syscall(pid.as_raw());

        loop {
            let status = wait::waitpid(pid, None).unwrap();
            let WaitStatus::Stopped(pid, sig) = status else {
                panic!("worker should be stopped (status = {:?})", status);
            };
            match sig {
                Signal::SIGTRAP => match check_syscall(pid.as_raw()) {
                    SyscallType::Exit => {
                        eprintln!("detected exit syscall");
                        ptrace_syscall(pid.as_raw());
                        let status = worker.wait().unwrap();
                        eprintln!("worker exited with code {}", status.code().unwrap());
                        break;
                    }
                    SyscallType::Authorized => {
                        eprintln!("detected authorized syscall");
                        ptrace_syscall(pid.as_raw());
                    }
                    SyscallType::Unauthorized(syscall_id) => {
                        eprintln!("detected unauthorized syscall with id {}", syscall_id);
                        worker.kill().unwrap();
                        eprintln!("worker killed");
                        return Box::pin(ready((
                            Err(DandelionError::UnauthorizedSyscall),
                            context,
                        )));
                    }
                },
                Signal::SIGSEGV => {
                    eprintln!("detected segmentation fault");
                    return Box::pin(ready((Err(DandelionError::SegmentationFault), context)));
                }
                s => {
                    eprintln!("detected {}", s);
                    return Box::pin(ready((Err(DandelionError::OtherProctionError), context)));
                }
            }
        }

        // erase all assumptions on context internal layout
        context.dynamic_data.clear();
        context.static_data.clear();
        // read outputs
        let result = get_output_layout(&mut context, &elf_config, worker_base_addr);
        self.is_running.store(false, Ordering::Release);
        Box::pin(ready((result, context)))
    }
    fn abort(&mut self) -> DandelionResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(DandelionError::NoRunningFunction);
        }
        Ok(())
    }
}

pub struct PagetableDriver {}

impl Driver for PagetableDriver {
    // // take or release one of the available engines
    fn start_engine(config: Vec<u8>) -> DandelionResult<Box<dyn Engine>> {
        if config.len() != 1 {
            return Err(DandelionError::ConfigMissmatch);
        }
        let cpu_slot: u8 = config[0];
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
            return Err(DandelionError::MalformedConfig);
        }
        // let (command_sender, command_receiver) = channel();
        // let (result_sender, result_receiver) = channel();
        // let thread_handle = spawn(move || run_thread(cpu_slot, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(Box::new(PagetableEngine {
            cpu_slot,
            // command_sender,
            // result_receiver,
            // thread_handle,
            is_running,
        }));
    }
}

const DEFAULT_SPACE_SIZE: usize = 0x80_0000; // 8MiB

pub struct PagetableLoader {}
impl Loader for PagetableLoader {
    // parses an executable,
    // returns the layout requirements and a context containing static data,
    //  and a layout description for it
    fn parse_function(
        function: Vec<u8>,
        static_domain: &Box<dyn MemoryDomain>,
    ) -> DandelionResult<Function> {
        let elf = elf_parser::ParsedElf::new(&function)?;
        let input_root = elf.get_symbol_by_name(&function, "inputRoot")?;
        let input_number = elf.get_symbol_by_name(&function, "inputNumber")?;
        let output_root = elf.get_symbol_by_name(&function, "outputRoot")?;
        let output_number = elf.get_symbol_by_name(&function, "outputNumber")?;
        let max_output_number = elf.get_symbol_by_name(&function, "maxOutputNumber")?;
        // let return_offset = elf.get_symbol_by_name(&function, "returnPair")?;
        let entry = elf.get_entry_point();
        let config = FunctionConfig::ElfConfig(super::ElfConfig {
            input_root: input_root,
            input_number: input_number,
            output_root: output_root,
            output_number: output_number,
            max_output_number: max_output_number,
            return_offset: (0, 0),
            entry_point: entry,
        });
        let (static_requirements, source_layout) = elf.get_layout_pair();
        // set default size to 128KiB
        let requirements = DataRequirementList {
            size: DEFAULT_SPACE_SIZE,
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
        let mut static_layout = Vec::<Position>::new();
        for position in source_layout.iter() {
            context.write(
                write_counter,
                function[position.offset..position.offset + position.size].to_vec(),
            )?;
            static_layout.push(Position {
                offset: write_counter,
                size: position.size,
            });
            write_counter += position.size;
        }
        context.static_data = static_layout;

        context.protection_requirements = elf.get_memory_protection_layout();
        return Ok(Function { requirements, context, config });
    }
}

#[cfg(test)]
mod test;
