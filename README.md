# Dandelion

Dandelion is a workernode system for serverless processing focused on maximizing elasticity.

In Dandelion applications are split into untrusted compute functions and trusted communication functions.
The compute functions are provided by users, while communication functions are built into the system.
A more in depth explanation can be found in the [paper here](https://arxiv.org/abs/2505.01603).
The [Dandelion SDK](https://github.com/eth-easl/dandelionSDK) describes the interfaces user functions can use in more detail.
Functions can be composed into graphs, where nodes are functions and edges describe which data is passed between them.
These compostions can be described using the composition language described in the README.md in the dparser folder.

# Structure

The main code for dandelion is organized in the following rust modules:
- **dandelion_commons** contains type definitions used throughout all of dandelion, such as error types and timestamps
- **machine_interface** has the isolation backend specific code, also has the code to detect and set machine specific options
  - **memory_domain** the code for interacting with the memory structures for setting up functions
  - **function_driver** the code for the engine, i.e. for running a function using a specific isolation technique
- **dparser** code for parsing compositions
- **dispatcher** registry and scheduler for the compositions, uses the domain and engine interferaces as well as the parser 
- **server** wrapper code arround the dispatcher for the http frontend

# Cargo 

The tests can be run using `cargo test`, which if executed in the top level directory will run tests for all the modules.
As a large number of tests need at least some engine enabled via a feature flag, only running test without any feature flags will not be very useful.
Usually we run tests with at least a compute engine feature and a communication engine feature enabled.
To build only a specific module (for example the machine interface), execue the command in the corresponding folder.
For more details on features see bellow.

To build the main server, the binary has to be specified using the `--bin dandelion_server` flag, giving the full command:
`cargo build --bin dandelion_server`, can also be run directly, by replacing build with run.
The server can also be run directly by replacing `build` with `run`.

# Features

To make it easy to enable / disable different backends we hide them behind feature flags.
All features are disabled by default and should be able to be enabled independently.
The server module currently assumes only computation and one communication engine feature to be enabled, but should be fixed in the future.

Feature flags for computation engines:
- `cheri` for enabling cheri backed isolation, requires a cheri capable compiler and hardware to run it.
- `kvm` for enabling kvm backed isolation, requires KVM module installed and user to have permissions to access `/dev/kvm`
- `mmu` for enabling process based isolation
- `wasm` for enabling rwasm based isolation

Feature flags for communcation engines:
- `reqwest_io` for enabling the reqwest based communication

Other features:

`timestamp` enables timestamping, this also enables http requests to `/stat` on the running server to get timestamps information. The timestamps are preallocated at the start and thus have a limited number. Currently running out of preallocated timestamps causes errors on requests. When the stat interface is accessed the used timestamps are cleared and put back into the pool of available ones.

Note: the `wasm` backend is currently based on rWasm, a reasearch project that seems to have been abandoned, so we may drop support for it in the future.

# Config

For the dandelion server we support configuration either via environ variables, command line arguments or a json config file.
To set the option <option> use `--<option>=<value>` as command line argument, `<OPTION>=<value>` (all uppercase) as enviromental variable or set `<option>:<value>` in the json.

The default search path for the config file is `./dandelion.config`, but can be set from environment or argument using `config_path`.

- `port` a u16 to define the port the dandelion server listens on, default: 8080
- `single_core_mode` allows all engines, frontend and dispatcher to run on the same core, intended only for testing, default: false
- `total_cores` max numbers of cores the server is allowed to use, default: number of physical cores
- `dispatcher cores` number of cores to use for the dispatcher, default: 1
- `frontend_cores` number of cores to use for the frontend
- `io_cores` number of cores to use for the communication engines, default: 0
- `timestamp_count` how many timestamps to preallocate, default: 1000

The number of compute engine cores is inferred from the total number of cores by deducting the number of other cores.

**WARNING** if 0 are allocated for the communication engines, the communication functions currently will hang indefinetly.

Additional configuration:

Dandelion respects `RUST_LOG` which can be used to set the log level, it can be set to the standard log levels: [`error`, `warn`, `info`, `debug`, `trace`]
For additional information consult the rust log and env_log modules. 

To use a `mmu_worker` that is not at the original location it was built in, set the `PROCESS_WORKER_PATH` environment variable to point to the desired binary

# MMU worker build

The `mmu_worker` binary required by the `MmuEngine` is assumed to be present in corresponding `target` directory:
```
cargo build --bin mmu_worker --features mmu --target $(arch)-unknown-linux-gnu [--release]
```
It is also recommended to statically link `mmu_worker` for a faster loading:
```
# x86_64
RUSTFLAGS='-C target-feature=+crt-static'
# aarch64
RUSTFLAGS='-C target-feature=+crt-static -C link-arg=-Wl,-fuse-ld=lld,--image-base=0xaaaaaaaa0000'
```
Also make sure that shared memory objects are executable:
```
sudo mount -o remount,exec /dev/shm
```

## Paper
If you use Dandelion, please cite our paper:
```bibtex
@misc{kuchler2025unlockingtrueelasticitycloudnative,
      title={Unlocking True Elasticity for the Cloud-Native Era with Dandelion}, 
      author={Tom Kuchler and Pinghe Li and Yazhuo Zhang and Lazar Cvetković and Boris Goranov and Tobias Stocker and Leon Thomm and Simone Kalbermatter and Tim Notter and Andrea Lattuada and Ana Klimovic},
      year={2025},
      eprint={2505.01603},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2505.01603}, 
}
```

# C Dependencies

For testing the C code to interact with Cheri we are using unity which is included directly in the project.
