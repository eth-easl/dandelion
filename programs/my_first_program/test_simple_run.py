import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dandelion_client import register_function, invoke_function

BINARY_PATH = Path(__file__).resolve().parent / "test_simple"

# Register the function
register_function(
    function_name="test_simple",
    function_path=str(BINARY_PATH),
    context_size=0x802_0000,
    engine_type="Kvm",
    input_sets=[],
    output_sets=["result"],
)

# Invoke it
result = invoke_function(
    function_name="test_simple",
    input_sets=[],
    output_parser=None,
)
print("RESULT:", result)
