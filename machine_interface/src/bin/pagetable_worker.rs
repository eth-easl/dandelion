use libc::{c_void, PF_X, PF_W};
use machine_interface::util::shared_mem::SharedMem;
use nix::sys::mman::{mprotect, ProtFlags};
use std::vec::Vec;
use machine_interface::Position;

fn main() {
    // get shared memory id from arguments
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 3);

    let mem_id = &args[1];

    eprintln!("worker started with shared memory {}", mem_id);

    // open and map a shared memory region
    let mem = SharedMem::open(
        mem_id,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
    )
    .unwrap();
    eprintln!("shared memory loaded");
    
    // send mapped address of shared memory to server
    println!("{}", mem.as_ptr() as usize);

    // receive the entry point of user's code from server
    let mut buf: String = String::new();
    std::io::stdin().read_line(&mut buf).unwrap();
    let entry_point: usize = buf.trim().parse().unwrap();
    eprintln!("get entry point {:x}", entry_point);

    // let (read_only, executable): (Vec<Position>, Vec<Position>) = serde_json::from_str(&args[2]).unwrap();
    let mut executable: Vec<(u32, Position)> = serde_json::from_str(&args[2]).unwrap();

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
                position.1.offset as isize) as *mut c_void, 
                position.1.size,
                flags
            ).expect("mprotect failed!");
        }
    }
    

    // jump to the entry point, then the process becomes untrusted
    unsafe {
        let user_main: fn() = std::mem::transmute(mem.as_ptr().offset(entry_point as isize));
        user_main();
    }
    eprintln!("function completed");
    // unreachable!();
}
