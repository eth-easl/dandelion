# dandelion

This project is aiming to build a workernode for serverless computing with
minimal, secure isolation.

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

The documentation can for all modules can be built by executing `cargo doc` in the top level directory.
To build only for a specific module, execue the command in the corresponding folder.

The tests can be run using `cargo test`, which if executed in the top level directory will run tests for all the modules.
As a large number of tests need at least some engine enabled via a feature flag, only running test without any feature flags will not run a lot.
To get most test coverage all viable features should be enabled.
For more details on features see bellow.

To build the main server, the binary has to be specified using the `--bin dandelion_server` flag, giving the full command:
`cargo build --bin dandelion_server`, can also be run directly, by replacing build with run.

# Features

To make it easy to enable / disable different backends we hide them behind feature flags.
All features are disabled by default and should be able to be enabled independently.
The server module currently assumes only computation and one communication engine feature to be enabled, but should be fixed in the future.

Feature flags for computation engines:
- `cheri` for enabling cheri backed isolation 
- `mmu` for enabling process based isolation
- `wasm` for enabling rwasm based isolation

Feature flags for communcation engines:
- `reqwest_io` for enabling the reqwest based communication

Other features:

`timestamp` enables timestamping, this also enables http requests to `/stat` on the running server to get timestamps information. The timestamps are preallocated at the start and thus have a limited number. Currently running out of preallocated timestamps causes errors on requests. When the stat interface is accessed the used timestamps are cleared and put back into the pool of available ones.

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

**WARNING** if no cores are allocated for the communication engines, the communication functions currently will hang indefinetly.

Additional configuration:

Dandelion respects `RUST_LOG` which can be used to set the log level, it can be set to the standard log levels: [`error`, `warn`, `info`, `debug`, `trace`]
For additional information consult the rust log and env_log modules. 

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

## MMU worker path

To use a `mmu_worker` that is not at the original location it was built in, set the `PROCESS_WORKER_PATH` environment variable to point to the desired binary

# C Dependencies

For testing the C code to interact with Cheri we are using unity which is included directly in the project.

# Cheri / BSD setup
This is mostly legacy information, but is kept for reproduction without cheri hardware running linux

## Cheri setup

First clone the Cheribuild repository onto a local disk.
It is important that it is on a local disk, as it needs to lock some files which
does not work with remote file systems.

```
git clone https://github.com/CTSRD-CHERI/cheribuild.git
```

Then go into the cheribuild folder and copy the cheribuild.json into it.
It contains configurations that make it easier to manage the output produced by
the build process and also manages the ssh-ports for the different builds.
Then build the sources with:
```
./cheribuild.py -d cheribsd-morello-purecap
```
This will generate the sources to make an image and install all dependencies.
If there are any errors consult the cheribuild git repo.

Then add your ssh keys to the configured source folder.
```
cd <cheri-source>/extra-files
mkdir root && cd root
mkdir .ssh && cd .ssh
cp <public-key-path> authorized_keys
```

Then you can build the disk image in the cheribsd folder:
```
./cheribuild.py disk-image-morello-purecap
```

And start it with either QEMU or FVP.
To run it for the first time add a -d at the end.
This will rebuild all dependencies (including the steps we already did)
If the image is already built and QEMU or FVP are installed separately then it
it can be executed right away.
```
./cheribuild.py run-morello-purecap
./cheribuild.py run-fvp-morello-purecap
```

To manually set the ssh-port to another port that the one set by the config
(55555) use `--run/ssh-forwarding-port=<portno>` or
`--run-fvp/ssh-port=<portno>` before the run command.

## Build

Before cross compilation can be used you need to fill in the respective paths
in the toolchain file.

If the toolchain still needs to be built, set up the cheribuild repository as
described above and build it with `./cheribuild sdk-morello-purecap -d`

After specifying them you can make a build folder, change in to it, and start
building with the toolchain file:
```
mkdir build
cd build
cmake -DCMAKE_TOOLCHAIN_FILE=../morello-toolchain.txt ..
make
```
For native builds the toolchain specification can be omitted.

## Kernel module build

The kernel modules needed are in the kernelModule folder.
They can be built with a normal make command, but have the following requirements:

- the /usr/src folder contains the OS source

## Known issues

- make: "/usr/src/sys/conf/kmod.mk" line 549: is ZFSTOP set?
  - solution `export ZFSTOP=/usr/src/sys/contrib/openzfs`
- currently syscall seems not to work, but loading does, with cpuset -l <core> a specific core can be setup. repeat for each core on machine to set up entire machine.

## GPU worker build

The `gpu_worker` binary required by the `gpu_process` is assumed to be present in corresponding `target` directory:
```
cargo build --bin gpu_worker --features gpu_process --target $(arch)-unknown-linux-gnu [--release]
```

Also make sure that shared memory objects are executable:
```
sudo mount -o remount,exec /dev/shm
```

### GPU worker path

To use a `gpu_worker` that is not at the original location it was built in, set the `GPU_WORKER_PATH` environment variable to point to the desired binary

## GPU engine library path
`DANDELION_LIBRARY_PATH` overwrites the directory where the GPU engines will look for kernel libraries. If the variable is unset the engines will look in `machine_interface/tests/libs/`.

## GPU Allocations
To prevent memory leakage, GPU kernels are disallowed from calling `malloc()`. All the memory a kernel requires should be specified in the respective config file.