use core_affinity::CoreId;
use machine_interface::util::shared_mem::SharedMem;
use nix::sys::{
    mman::{mprotect, ProtFlags},
    ptrace,
};
use std::vec::Vec;
use machine_interface::Position;

// these flags are universal for elf files,
// but for some reason only appear in libc crate on linux
const PF_X: u32 = 1 << 0;
const PF_W: u32 = 1 << 1;
const PF_R: u32 = 1 << 2;

fn main() {
    // get shared memory id from arguments
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 4);

    let core_id: usize = args[1].parse().unwrap();
    let mem_id = &args[2];
    eprintln!("[worker] started with core {} and shared memory {}", core_id, mem_id);

    // set cpu affinity
    assert!(core_affinity::set_for_current(CoreId { id: core_id }));

    // open and map a shared memory region
    let mem = SharedMem::open(
        mem_id,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
    )
    .unwrap();
    eprintln!("[worker] loaded shared memory");

    // send mapped address of shared memory to server
    println!("{}", mem.as_ptr() as usize);

    // receive the entry point of user's code from server
    let mut buf: String = String::new();
    std::io::stdin().read_line(&mut buf).unwrap();
    let entry_point: usize = buf.trim().parse().unwrap();
    eprintln!("[worker] got entry point {:x}", entry_point);

    // let (read_only, executable): (Vec<Position>, Vec<Position>) = serde_json::from_str(&args[3]).unwrap();
    let mut executable: Vec<(u32, Position)> = serde_json::from_str(&args[3]).unwrap();

    for position in executable.iter_mut() {
        unsafe {
            let mut flags: ProtFlags = ProtFlags::PROT_READ;
            if position.0 & PF_X == PF_X {
                flags |= ProtFlags::PROT_EXEC;
            }
            if position.0 & PF_W == PF_W {
                flags |= ProtFlags::PROT_WRITE
            }

            mprotect(mem.as_ptr().offset(
                position.1.offset as isize) as *mut libc::c_void, 
                position.1.size,
                flags
            ).expect("mprotect failed!");
        }
    }

    // renounce ability to invoke syscalls by ptrace
    ptrace::traceme().unwrap();
    unsafe {
        let res = libc::kill(libc::getpid(), libc::SIGSTOP);
        assert_eq!(res, 0);
    }

    // jump to the entry point, then the process becomes untrusted
    unsafe {
        let user_main: fn() = std::mem::transmute(mem.as_ptr().offset(entry_point as isize));
        user_main();
        libc::exit(0);
    }
}
