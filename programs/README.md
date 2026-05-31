# Writing Dandelion Programs

## Prerequisites

- Dandelion server running with KVM support
- Dandelion SDK cloned (e.g., `/home/lmo/dandelionSDK`)
- Docker (for SDK compilation)
- Python 3 with `bson` and `requests` installed

## Start the Server

In a dedicated terminal, run:

```bash
RUST_LOG=debug cargo run --bin dandelion_server -F kvm,reqwest_io,timestamp,log_function_stdio,vendored-ssl --release -- --config-path programs/minimal/dandelion.config
```

Keep this terminal open. The server must stay alive.

## Python Client

Place your program in a subfolder under `programs/`. Import `dandelion_client` from the parent directory:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dandelion_client import register_function, invoke_function
```

### API

- `register_function(name, function_path, context_size, engine_type, input_sets, output_sets)` — Registers a binary.
- `invoke_function(name, input_sets, output_parser)` — Runs it and returns a BSON dict.

Optional composition helpers:
- `register_composition(dsl_string)` — Registers a composition DSL.
- `invoke_composition(name, input_sets, output_parser)` — Runs a composition.

## Part 1: Using an Existing Binary

The repository includes pre-built test binaries in `machine_interface/tests/data/`.

```python
from dandelion_client import register_function, invoke_function

FUNCTION_PATH = (
    Path(__file__).resolve().parent
    / "../../machine_interface/tests/data/test_elf_kvm_x86_64_matmul"
)

# Register the function
register_function(
    function_name="matmul",
    function_path=str(FUNCTION_PATH),
    context_size=0x802_0000,
    engine_type="Kvm",
    input_sets=[["InMats", None]],
    output_sets=["OutMats"],
)

# Invoke it
result = invoke_function(
    function_name="matmul",
    input_sets=[{
        "identifier": "",
        "items": [{
            "identifier": "",
            "key": 0,
            "data": b"your_input_data_here"
        }]
    }],
    output_parser=None
)
print(result)
```

## Part 2: Building a New Binary from Scratch

### Step 1: Build the SDK for KVM

The SDK is pre-built for `debug` platform. You must rebuild it for `kvm`. The `dev_rebuild.sh` script handles a clean build, including creating the compiler wrapper.

```bash
# Navigate to the SDK
cd /home/lmo/dandelionSDK

# Start the dev container (has the compiler)
./dev/dev_container.sh

# Inside the container, rebuild the SDK for KVM
./dev/dev_rebuild.sh sdk --platform kvm
```

### Step 2: Write Your C Program

Create a file (e.g., `test_string.c`). The entry point must use the `DANDELION_ENTRY` macro.

```c
#include "dandelion/crt.h"
#include "dandelion/runtime.h"

static char output_buffer[] = "test string 123";

int main() {
  IoBuffer output = {
    .ident = "result",
    .ident_len = 6,
    .data = output_buffer,
    .data_len = sizeof(output_buffer) - 1,
    .key = 0
  };
  dandelion_add_output(0, output);
  return 0;
}

DANDELION_ENTRY(main)
```

### Step 3: Set Up CMake

Create a `CMakeLists.txt` in the same folder:

```cmake
cmake_minimum_required(VERSION 3.23)
project(my_program LANGUAGES C)

# Point to the pre-installed SDK inside the container
add_subdirectory(/work/dandelion_sdk dandelion_sdk)

add_executable(test_string test_string.c)
target_compile_options(test_string PRIVATE -fno-stack-protector -O0)
target_link_options(test_string PRIVATE -static)
target_link_libraries(test_string PRIVATE dandelion_runtime)
```

### Step 4: Build Inside the Docker Container

```bash
# Copy your source folder to the container
docker cp /path/to/your/project dev_dandelion_sdk_container:/tmp/my_project

# Build inside the container
# Note: The Dandelion SDK compiler is a wrapper around Clang. The CMake command must
# specify the correct compiler to avoid the default GCC being used with the SDK flags.
docker exec -w /tmp/my_project dev_dandelion_sdk_container \
  cmake -B build -DCMAKE_C_COMPILER=/usr/lib/llvm-20/bin/clang

docker exec -w /tmp/my_project dev_dandelion_sdk_container \
  cmake --build build

# Copy the binary back
docker cp dev_dandelion_sdk_container:/tmp/my_project/build/test_string \
  /path/to/your/project/test_string
```

### Step 5: Register and Invoke

```python
from pathlib import Path
from dandelion_client import register_function, invoke_function

BINARY_PATH = Path(__file__).resolve().parent / "test_string"

register_function(
    function_name="test_string",
    function_path=str(BINARY_PATH),
    context_size=0x802_0000,
    engine_type="Kvm",
    input_sets=[["Input", None]],
    output_sets=["Output"],
)

result = invoke_function(
    function_name="test_string",
    input_sets=[{
        "identifier": "",
        "items": [{
            "identifier": "",
            "key": 0,
            "data": b"Hello"
        }]
    }],
    output_parser=None
)
print(result)
```

Expected output:
```python
{
    'sets': [{
        'identifier': 'Output',
        'items': [{
            'identifier': 'result',
            'key': 0,
            'data': b'test string 123'
        }]
    }],
    'timestamps': '...'
}
```
