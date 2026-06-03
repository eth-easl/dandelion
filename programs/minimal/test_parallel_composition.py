import argparse
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# dandelion_client lives in the parent directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dandelion_client import register_function, register_composition, invoke_composition


FUNCTION_PATH = (
    Path(__file__).resolve().parent
    / "../../machine_interface/tests/data/test_elf_kvm_x86_64_matmul"
)


def build_parallel_composition(num_parallel: int = 10, num_stages: int = 1000) -> str:
    """Build a composition with parallel calls at every stage.

    Each stage has `num_parallel` independent calls.
    All calls are necessary: each stage's call i reads from the previous stage's call i.
    """
    lines = ["function matmul(InMats) => (OutMats);", ""]
    lines.append(f"composition parallel_matmul(InputMats) => (FinalMats) {{")

    for stage in range(num_stages):
        for i in range(num_parallel):
            if stage == 0:
                src = "InputMats"
            else:
                src = f"Stage_{stage-1}_{i}"

            if stage == num_stages - 1 and i == num_parallel - 1:
                dst = "FinalMats"
            else:
                dst = f"Stage_{stage}_{i}"
            lines.append(f"    matmul(InMats = all {src}) => ({dst} = OutMats);")

    lines.append("}")
    return "\n".join(lines)


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
    parser.add_argument(
        "--register",
        action="store_true",
        help="Register the function and composition before invoking",
    )
    parser.add_argument(
        "--abort",
        action="store_true",
        help="Abort after 1 second by passing a 1s timeout to the request",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Register and then abort (1s timeout)",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print the composition to stdout and exit",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=10,
        help="Number of parallel matmul calls in the first stage (default: 10)",
    )
    parser.add_argument(
        "--chain",
        type=int,
        default=1000,
        help="Number of chained matmul calls after the parallel stage (default: 1000)",
    )
    args = parser.parse_args()

    COMPOSITION = build_parallel_composition(args.parallel, args.chain)

    if args.print:
        print(COMPOSITION)
        sys.exit(0)

    if not FUNCTION_PATH.exists():
        raise FileNotFoundError(f"Function binary not found: {FUNCTION_PATH}")

    start = time.time()
    if args.register or args.all:
        register_function(
            function_name="matmul",
            function_path=str(FUNCTION_PATH),
            context_size=0x802_0000,
            engine_type="Kvm",
            input_sets=[["InMats", None]],
            output_sets=["OutMats"],
        )
        register_composition(COMPOSITION)
    if not args.register or args.all:
        try:
            invoke_composition(
                composition_name="parallel_matmul",
                input_sets=build_matmul_input(3),
                output_parser=parse_matmul_output,
                timeout=1 if (args.abort or args.all) else 10,
            )
        except Exception as e:
            print(
                f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] Request failed: {e}"
            )
    print(f"execution time: {time.time() - start:.3f}s")
