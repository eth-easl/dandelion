use std::io;

use core_affinity::CoreId;
use dandelion_commons::DandelionResult;
use machine_interface::{
    function_driver::{
        compute_driver::gpu::{gpu_utils::SendFunctionArgs, GpuLoop},
        thread_utils::EngineLoop,
        ComputeResource, FunctionConfig,
    },
    memory_domain::Context,
};

fn main() {
    // parse args
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 4);
    let core_id: u8 = args[1].parse().expect("Invalid core ID");
    let gpu_id: u8 = args[2].parse().expect("Invalid GPU ID");
    let worker_count: u8 = args[3].parse().expect("Invalid worker count");

    // set cpu affinity
    assert!(core_affinity::set_for_current(CoreId {
        id: core_id as usize
    }));

    // setup worker struct
    let mut worker = GpuLoop::init(ComputeResource::GPU(core_id, gpu_id, worker_count))
        .expect("Should be able to create worker");

    // unwrap okay, as all lines are valid Strings
    for inp in io::stdin().lines().map(|l| l.unwrap()) {
        match execute(&mut worker, inp) {
            Ok(()) => {
                println!("__OK__");
            }
            Err(e) => {
                println!("__ERROR__ {:?}", e);
            }
        }
    }
}

fn execute(worker: &mut GpuLoop, inp: String) -> DandelionResult<()> {
    let SendFunctionArgs {
        config,
        context,
        output_sets,
    } = serde_json::from_str(&inp).expect("Parsing function args failed");

    let config = FunctionConfig::GpuConfig(config);
    let context: Context = context.try_into()?;

    worker.run(config, context, output_sets).map(|_| ())
}
