use core_affinity::CoreId;
use machine_interface::Position;
use machine_interface::{memory_domain::mmu::MMAP_BASE_ADDR, util::shared_mem::SharedMem};
use nix::sys::{
    mman::{mprotect, ProtFlags},
    ptrace,
};
use std::arch::asm;
use std::vec::Vec;

// these flags are universal for elf files,
// but for some reason only appear in libc crate on linux
const PF_X: u32 = 1 << 0;
const PF_W: u32 = 1 << 1;

fn main() {
    // get shared memory id from arguments
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 5);

    let core_id: usize = args[1].parse().unwrap();
    let mem_id = &args[2];
    let entry_point: usize = args[3].parse().unwrap();
    // eprintln!("[worker] started with core {}, shared memory {} and entry point {:#x}", core_id, mem_id, entry_point);

    // set cpu affinity
    assert!(core_affinity::set_for_current(CoreId { id: core_id }));

    // open and map a shared memory region
    let mem = match SharedMem::open(
        mem_id,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
        MMAP_BASE_ADDR,
    ) {
        Ok(m) => m,
        Err(e) => {
            if e == nix::errno::Errno::EEXIST {
                // POTENTIAL ADDRESS COLLISION BETWEEN WORKER AND USER'S FUNCTION!
                // CHECK "TODO: modify ELF header"
                eprintln!("address {:#x} in use", MMAP_BASE_ADDR);
            }
            panic!("{}", e);
        }
    };
    // eprintln!("[worker] loaded shared memory");

    // set pagetable protection flags
    // TODO: make sure get_free_space return a rw region
    let protection_requirements: Vec<(u32, Position)> = serde_json::from_str(&args[4]).unwrap();
    for position in protection_requirements.iter() {
        unsafe {
            let mut flags: ProtFlags = ProtFlags::PROT_READ;
            if position.0 & PF_X == PF_X {
                flags |= ProtFlags::PROT_EXEC;
            }
            if position.0 & PF_W == PF_W {
                flags |= ProtFlags::PROT_WRITE
            }
            mprotect(
                position.1.offset as *mut libc::c_void,
                position.1.size,
                flags,
            )
            .expect("mprotect failed!");
        }
    }

    // renounce ability to invoke syscalls by ptrace
    ptrace::traceme().unwrap();
    unsafe {
        let res = libc::kill(libc::getpid(), libc::SIGSTOP);
        assert_eq!(res, 0);
    }

    let stack_pointer = mem.len() - 32;

    // jump to the entry point, then the process becomes untrusted
    run_user_code(entry_point, stack_pointer);
}

fn run_user_code(entry_point: usize, stack_pointer: usize) -> ! {
    unsafe {
        // TODO: clear registers
        // TODO: implement this in asm file(s) so that compiler won't mess up with registers?
        #[cfg(target_arch = "x86_64")]
        asm!(
            "mov rax, {0}",
            "mov rsp, {1}",
            "jmp rax",
            in(reg) entry_point,
            in(reg) stack_pointer,
        );
        #[cfg(target_arch = "aarch64")]
        asm!(
            "mov x0, {0}",
            "mov sp, {1}",
            "blr x0",
            in(reg) entry_point,
            in(reg) stack_pointer,
        );
    }
    unreachable!();
}
