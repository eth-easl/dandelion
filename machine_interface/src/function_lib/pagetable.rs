use crate::{
    function_lib::{Driver, ElfConfig, Engine, FunctionConfig, Loader},
    memory_domain::{Context, ContextTrait, ContextType, MemoryDomain},
    util::elf_parser,
    DataItem, DataItemType, DataRequirement, DataRequirementList, HardwareError, HwResult,
    Position,
};
use core_affinity;
use futures::future::ready;
use nix::{
    sys::{
        signal::Signal,
        wait::{self, WaitStatus},
    },
    unistd::Pid,
};
use std::{
    io::{BufRead, BufReader, Write},
    pin::Pin,
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
};

const IO_STRUCT_SIZE: usize = 16;
const MAX_OUTPUTS: u32 = 16;

fn setup_input_structs(
    context: &mut Context,
    config: &ElfConfig,
    base_addr: usize,
) -> HwResult<()> {
    // size of array with input struct array
    // let pagetable_context = match &context.context {
    //     ContextType::Pagetable(c) => c,
    //     _ => return Err(HardwareError::ConfigMissmatch)
    // };

    let input_number_option = context
        .dynamic_data
        .iter()
        .max_by(|a, b| a.index.cmp(&b.index));
    let input_number = match input_number_option {
        Some(num) => num.index + 1,
        None => return Ok(()),
    };
    let input_struct_size = input_number as usize * IO_STRUCT_SIZE;
    let input_offset = context.get_free_space(input_struct_size, 8)?;
    // let base_addr = pagetable_context.storage.as_ptr();

    for input in &context.dynamic_data {
        let pos = match &input.item_type {
            DataItemType::Item(position) => position,
            _ => return Err(HardwareError::NotImplemented),
        };
        let struct_offset = input_offset + IO_STRUCT_SIZE * input.index as usize;
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
        u32::to_ne_bytes(input_number).to_vec(),
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

fn get_output_layout(context: &mut Context, config: &ElfConfig, base_addr: usize) -> HwResult<()> {
    // get output number
    let output_number_vec = context.read(config.output_number.0, config.output_number.1, false)?;
    // TODO make this dependent on the actual size of the values
    // TODO use as_chunks when it stabilizes
    let output_number_slice: [u8; 4] = output_number_vec[0..4]
        .try_into()
        .expect("Should have correct length");
    let output_number = u32::min(u32::from_ne_bytes(output_number_slice), MAX_OUTPUTS);
    let mut output_structs = Vec::<DataItem>::new();
    let output_root_vec = context.read(config.output_root.0, 8, false)?;
    let output_root_offset = usize::from_ne_bytes(
        output_root_vec[0..8]
            .try_into()
            .expect("Should have correct length"),
    );

    // let pagetable_context = match &context.context {
    //     ContextType::Pagetable(c) => c,
    //     _ => return Err(HardwareError::ConfigMissmatch)
    // };
    // let base_addr = pagetable_context.storage.as_ptr();

    for output_index in 0..output_number {
        // let read_offset = output_root_offset + IO_STRUCT_SIZE * output_index as usize;
        let read_offset = (output_root_offset + IO_STRUCT_SIZE * output_index as usize
            - base_addr as usize) as usize;
        let out_struct = context.read(read_offset, IO_STRUCT_SIZE as usize, false)?;
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
        output_structs.push(DataItem {
            index: output_index,
            item_type: DataItemType::Item(Position {
                offset: offset - base_addr as usize,
                size: size,
            }),
        })
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
    // result_receiver: Receiver<HwResult<()>>,
    // thread_handle: JoinHandle<()>,
}

// fn run_thread(
//     core_id: u8,
//     // command_receiver: Receiver<CheriCommand>,
//     result_sender: Sender<HwResult<()>>,
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
//             1 => Err(HardwareError::OutOfMemory),
//             _ => Err(HardwareError::NotImplemented),
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

#[cfg(target_os = "linux")]
fn check_syscall_non_exit(pid: libc::pid_t) -> Option<i64> {
    type Regs = libc::user_regs_struct;
    let regs = unsafe {
        let mut regs_uninit: core::mem::MaybeUninit<Regs> = core::mem::MaybeUninit::uninit();
        let res: i64 = libc::ptrace(libc::PTRACE_GETREGS, pid, 0, regs_uninit.as_mut_ptr());
        assert_eq!(res, 0);
        regs_uninit.assume_init()
    };
    #[cfg(target_arch = "x86_64")]
    let syscall_id = regs.orig_rax as i64;
    #[cfg(target_arch = "aarch64")]
    let syscall_id = regs.regs[0] as i64;
    match syscall_id {
        libc::SYS_exit | libc::SYS_exit_group => None,
        id => Some(id),
    }
}

#[cfg(target_os = "freebsd")]
fn check_syscall_non_exit(pid: libc::pid_t) -> Option<i64> {
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
        1 => None,
        id => Some(id),
    }
}

impl Engine for PagetableEngine {
    fn run(
        &mut self,
        config: &FunctionConfig,
        mut context: Context,
    ) -> Pin<Box<dyn futures::Future<Output = (HwResult<()>, Context)> + '_>> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return Box::pin(ready((Err(HardwareError::EngineAlreadyRunning), context)));
        }
        let elf_config = match config {
            FunctionConfig::ElfConfig(conf) => conf,
            _ => return Box::pin(ready((Err(HardwareError::ConfigMissmatch), context))),
        };
        let pagetable_context = match &context.context {
            ContextType::Pagetable(pagetable_context) => pagetable_context,
            _ => return Box::pin(ready((Err(HardwareError::ContextMissmatch), context))),
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
        //     Err(_) => return (Err(HardwareError::EngineError), context),
        //     Ok(_) => (),
        // }
        // match self.result_receiver.recv() {
        //     Err(_) => return (Err(HardwareError::EngineError), context),
        //     Ok(Err(err)) => return (Err(err), context),
        //     Ok(Ok(())) => (),
        // }

        // create a new address space (child process) and pass the shared memory
        let mut worker = Command::new("target/debug/pagetable_worker")
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

        let status = wait::waitpid(pid, None).unwrap();
        let WaitStatus::Stopped(pid, sig) = status else {
            panic!("worker should be stopped (status = {:?})", status);
        };
        match sig {
            Signal::SIGTRAP => match check_syscall_non_exit(pid.as_raw()) {
                None => {
                    eprintln!("detected exit syscall");
                    ptrace_syscall(pid.as_raw());
                    let status = worker.wait().unwrap();
                    eprintln!("worker exited with code {}", status.code().unwrap());
                }
                Some(syscall_id) => {
                    eprintln!("detected unauthorized syscall with id {}", syscall_id);
                    worker.kill().unwrap();
                    eprintln!("worker killed");
                    return Box::pin(ready((Err(HardwareError::UnauthorizedSyscall), context)));
                }
            },
            Signal::SIGSEGV => {
                eprintln!("detected segmentation fault");
                return Box::pin(ready((Err(HardwareError::SegmentationFault), context)));
            }
            s => {
                eprintln!("detected {}", s);
                return Box::pin(ready((Err(HardwareError::OtherProctionError), context)));
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
    fn abort(&mut self) -> HwResult<()> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(HardwareError::NoRunningFunction);
        }
        Ok(())
    }
}

pub struct PagetableDriver {
    cpu_slots: Vec<u8>,
}

impl Driver for PagetableDriver {
    // required parts of the trait
    type E = PagetableEngine;
    fn new(config: Vec<u8>) -> HwResult<Self> {
        // each entry in config is expected to be a core id for a cpu to use
        // check that each one is available
        let available_cores = match core_affinity::get_core_ids() {
            None => return Err(HardwareError::EngineError),
            Some(cores) => cores,
        };
        for core in config.iter() {
            let found = available_cores
                .iter()
                .find(|x| x.id as u8 == core.clone())
                .is_some();
            if !found {
                return Err(HardwareError::MalformedConfig);
            }
        }
        return Ok(PagetableDriver { cpu_slots: config });
    }
    // // take or release one of the available engines
    fn start_engine(&mut self) -> HwResult<Self::E> {
        let cpu_slot = match self.cpu_slots.pop() {
            Some(core_id) => core_id,
            None => return Err(HardwareError::NoEngineAvailable),
        };
        // let (command_sender, command_receiver) = channel();
        // let (result_sender, result_receiver) = channel();
        // let thread_handle = spawn(move || run_thread(cpu_slot, result_sender));
        let is_running = AtomicBool::new(false);
        return Ok(PagetableEngine {
            cpu_slot,
            // command_sender,
            // result_receiver,
            // thread_handle,
            is_running,
        });
    }
    fn stop_engine(&mut self, engine: Self::E) -> HwResult<()> {
        // drop(engine.command_sender);
        // drop(engine.result_receiver);
        // TODO check if expect makes sense
        // engine
        //     .thread_handle
        //     .join()
        //     .expect("Expecting cheri thread handle to be joinable");
        self.cpu_slots.push(engine.cpu_slot);
        return Ok(());
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
        static_domain: &mut dyn MemoryDomain,
    ) -> HwResult<(DataRequirementList, Context, FunctionConfig)> {
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
        return Ok((requirements, context, config));
    }
}

#[cfg(test)]
mod test;
