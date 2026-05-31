import argparse
import struct
import sys
from pathlib import Path

# dandelion_client lives in the parent directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dandelion_client import register_function, register_composition, invoke_composition


FUNCTION_PATH = (
    Path(__file__).resolve().parent
    / "../../machine_interface/tests/data/test_elf_kvm_x86_64_matmul"
)

COMPOSITION = """
function matmul(InMats) => (OutMats);

composition double_matmul(InputMats) => (FinalMats) {
    matmul(InMats = all InputMats) => (TempMats = OutMats);
    matmul(InMats = all TempMats) => (FinalMats = OutMats);
}
"""


def build_matmul_input(matrix_dim: int) -> list[dict]:
    """Build input sets for a matrix multiplication function."""
    data = struct.pack("<Q", matrix_dim) + b"".join(
        struct.pack("<Q", 1) for _ in range(matrix_dim * matrix_dim)
    )
    return [
        {
            "identifier": "",
            "items": [
                {
                    "identifier": "",
                    "key": 0,
                    "data": data,
                }
            ],
        }
    ]


def parse_matmul_output(decoded: dict) -> tuple:
    """Extract u64 values from a matmul response."""
    out = decoded["sets"][0]["items"][0]["data"]
    values = struct.unpack("<" + "Q" * (len(out) // 8), out)
    print("decoded u64s:", values)
    return values


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--register", action="store_true", help="Register the function and composition before invoking")
    args = parser.parse_args()

    if not FUNCTION_PATH.exists():
        raise FileNotFoundError(f"Function binary not found: {FUNCTION_PATH}")

    if args.register:
        register_function(
            function_name="matmul",
            function_path=str(FUNCTION_PATH),
            context_size=0x802_0000,
            engine_type="Kvm",
            input_sets=[["InMats", None]],
            output_sets=["OutMats"],
        )
        register_composition(COMPOSITION)
    else:
        invoke_composition(
            composition_name="double_matmul",
            input_sets=build_matmul_input(3),
            output_parser=parse_matmul_output,
        )
