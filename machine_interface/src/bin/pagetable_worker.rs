use machine_interface::util::shared_mem::SharedMem;
use nix::sys::mman::ProtFlags;

fn main() {
    // get shared memory id from arguments
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 2);
    let mem_id = &args[1];
    eprintln!("worker started with shared memory {}", mem_id);

    // open and map a shared memory region
    let mem = SharedMem::open(
        mem_id,
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE | ProtFlags::PROT_EXEC,
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

    // jump to the entry point, then the process becomes untrusted
    unsafe {
        let user_main: fn() = std::mem::transmute(mem.as_ptr().offset(entry_point as isize));
        user_main();
    }
    eprintln!("function completed");
    // unreachable!();
}
