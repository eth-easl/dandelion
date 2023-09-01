#![crate_type = "staticlib"]
use core_affinity::CoreId;
use machine_interface::{util::shared_mem::SharedMem, Position};
use nix::sys::{
    mman::{ProtFlags, MapFlags, mmap, mprotect}, ptrace,
};
use std::vec::Vec;


use std::num::NonZeroUsize;
use libc;

// these flags are universal for elf files,
// but for some reason only appear in libc crate on linux
const PF_X: u32 = 1 << 0;
const PF_W: u32 = 1 << 1;

fn next_steps(mem_id:&String, json_file:&String, addr:usize) {
    // open and map a shared memory region
    let mem = SharedMem::open(
        mem_id,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
        addr
    )
    .unwrap();
    eprintln!("[worker] loaded shared memory");

    // send mapped address of shared memory to server
    //println!("{}", mem.as_ptr() as usize);

    // receive the entry point of user's code from server
    let mut buf: String = String::new();
    std::io::stdin().read_line(&mut buf).unwrap();
    let entry_point: usize = buf.trim().parse().unwrap();
    eprintln!("[worker] got entry point {:x}", entry_point);

    // let (read_only, executable): (Vec<Position>, Vec<Position>) = serde_json::from_str(&args[3]).unwrap();
    let mut executable: Vec<(u32, Position)> = serde_json::from_str(json_file).unwrap();

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

// pub fn panic_handler_generate_coredump() {
//     let default_panic = std::panic::take_hook();

//     std::panic::set_hook(Box::new(move |panic_info| {
//         default_panic(panic_info);

//         let pid = std::process::id();

//         use libc::kill;
//         use libc::SIGQUIT;

//         use std::convert::TryInto;
//         unsafe { kill(pid.try_into().unwrap(), SIGQUIT) };
//     }));
// }

fn main() {
    //for debuging
    //panic_handler_generate_coredump();

    // get shared memory id from
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 5);

    let core_id: usize = args[1].parse().unwrap();
    let mem_id = &args[2];
    eprintln!("[worker] started with core {} and shared memory {}", core_id, mem_id);
    let json_file = &args[4];

    // set cpu affinity
    assert!(core_affinity::set_for_current(CoreId { id: core_id }));

    let _user_code_len:usize = args[3].parse().unwrap();

    let code_len = 0xA3A80;
    let new_addr = 0x560b3f8a1000;
    
    // get next_steps offset
    let next_steps_ptr: fn(&String, &String, usize) = next_steps;
    let next_steps_ptr = next_steps_ptr as *const () as usize;
    println!("address {}", next_steps_ptr);
    let next_steps_ptr = next_steps_ptr as usize;

    // create new memory domain to copy the init code
    let new_memory = unsafe {
        mmap(
            NonZeroUsize::new(new_addr), 
            NonZeroUsize::new(code_len).unwrap(),
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE | ProtFlags::PROT_EXEC,
            MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS| MapFlags::MAP_FIXED,
            -1,
            0,
        )
    };

    let _result = match new_memory{
        Err(err) => println!("map failed with {:?}", err),
    
        Ok(memory) => println!("not failed {:?}", memory)
    };

    let new_memory = new_memory.unwrap();

    // get start address
    let src = next_steps_ptr-0x12f30;
    println!("{}", src);

    unsafe {
        std::ptr::copy_nonoverlapping(src as *const u8, new_memory as *mut u8, 83792);
    }
    eprintln!("[worker] copying completed successfully");

    let relocated_function: extern "C" fn(&String, &String, usize) = unsafe { std::mem::transmute(new_memory as usize + 0x13790) };
    relocated_function(mem_id, json_file, src);
}