# Sidecar
Currently it only supports HTTP/2.

## Clone the repo
```
git clone --branch dev/sidecar --single-branch https://github.com/eth-easl/dandelion.git
cd ./dandelion/server
```

## Build the codec
Follow the instructions in `https://github.com/eth-easl/dandelion_service_mesh/tree/main/codec_filters` to get the codec executable file.

Specify the codec executable file path in `config.rs`
```
const DEFAULT_NGHTTP2_CODEC_BIN_LOCAL_PATH: &str = PATH_TO_YOUR_CODEC_EXECUTABLE;
```

## Install the Rust and dependencies

```
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev

curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh

source "$HOME/.cargo/env"

```

## Before start
In `dirigent_service.rs`, the func `create_dirigent_server`, lines 234,235, I temporarily added two entries in the Dirigent service for initial testing:
```
process_add_action(Arc::clone(&dirigent_service.data), String::from("warm-function-4949985443906962520"), String::from("111"), String::from("localhost:5555"));
process_add_action(Arc::clone(&dirigent_service.data), String::from("warm-function-4949985443906962521"), String::from("222"), String::from("localhost:5556"));
```
Remember to remove them.

### kvm isolation
For the `kvm` isolation, KVM module is required to be installed.

And the user should be granted the permission to access `/dev/kvm`
```
sudo usermod -aG kvm $USER
newgrp kvm   # refresh groups without logout
```

### mmu isolation
The `mmu_worker` binary required by the `MmuEngine` is assumed to be present in corresponding `target` directory.

The command is run under the `dandelion` folder:

For `x86_64`:
```
# x86_64
RUSTFLAGS='-C target-feature=+crt-static' cargo build --bin mmu_worker --features mmu --target x86_64-unknown-linux-gnu [--release]
```

For `aarch64`:
```
# aarch64
 RUSTFLAGS='-C target-feature=+crt-static -C link-arg=-Wl,-fuse-ld=lld,--image-base=0xaaaaaaaa0000' cargo build --bin mmu_worker --features mmu --target aarch64-unknown-linux-gnu [--release]
```

Also make sure that shared memory objects are executable:
```
sudo mount -o remount,exec /dev/shm
```


## Start the Sidecar

Now we can start the sidecar.

If use `KVM`:
```
RUST_LOG=debug cargo run --bin dandelion_server --features  "kvm reqwest_io" --release
```

If use `MMU`:
```
RUST_LOG=debug cargo run --bin dandelion_server --features  "mmu reqwest_io" --release
```

We can also assign more cpu cores to the run time (front-end). 

But of course this would leave less cores for the context (where the nghttp codec and other network filters run).

For example:
```
RUST_LOG=debug cargo run --bin dandelion_server --features  "kvm reqwest_io"  --release -- --frontend-cores=2
```

If you want to see more logs, run it in the debug mode (but with worse preformance):
```
RUST_LOG=debug cargo run --bin dandelion_server --features  "kvm reqwest_io"
```
