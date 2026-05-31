import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dandelion_client import register_function, invoke_function

# Assumes the binary was built with CMake inside the SDK container
BINARY_PATH = Path(__file__).resolve().parent / "echo"

if not BINARY_PATH.exists():
    raise FileNotFoundError(
        f"Binary not found: {BINARY_PATH}\n"
        "Build it first inside the SDK container:\n"
        "  cmake -B build -DCMAKE_C_COMPILER=/usr/lib/llvm-20/bin/clang\n"
        "  cmake --build build"
    )

register_function(
    function_name="echo",
    function_path=str(BINARY_PATH),
    context_size=0x802_0000,
    engine_type="Kvm",
    input_sets=[["input", None]],
    output_sets=["output"],
)

result = invoke_function(
    function_name="echo",
    input_sets=[{
        "identifier": "input",
        "items": [{
            "identifier": "input",
            "key": 0,
            "data": b"Hello, Dandelion!"
        }]
    }],
    output_parser=None
)
print("Result:", result)
