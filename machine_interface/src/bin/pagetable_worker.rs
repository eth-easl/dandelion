use machine_interface::util::shared_mem::SharedMem;
use nix::sys::mman::ProtFlags;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 2);
    let mem_id = &args[1];

    // get shared memory id from arguments
    let mem = SharedMem::open(mem_id, ProtFlags::PROT_READ).unwrap();

    // just a "placeholder" here
    for _ in 0..3 {
        std::thread::sleep(Duration::from_secs(1));
        unsafe {
            println!("{}", mem.as_slice()[0]);
        }
    }

    // // todo: add useful functions, e.g. jumping to the designated entry point:
    // let mut buf = String::new();
    // std::io::stdin().read_line(&mut buf).unwrap();
    // let entry_point: usize = buf.trim().parse().unwrap();
    // unsafe {
    //     let user_main: fn() = std::mem::transmute(entry_point);
    //     user_main();
    // }
    // unreachable!();
}
