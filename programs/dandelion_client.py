import struct
from typing import Optional, Callable

from bson import BSON
import requests
from requests.exceptions import ConnectionError

BASE_URL = "http://localhost:8080"

def register_function(function_name: str, function_path: str, context_size: int, engine_type: str="Kvm", input_sets: list[tuple[str, list[tuple[str, bytes]]]]=[], output_sets: list[str]=[], binary: list[int]=[]) -> None:
    payload = {
        "name": function_name,
        "context_size": context_size,
        "engine_type": engine_type,
        "local_path": function_path,
        # ignored when local_path is set
        "binary": binary,
        "input_sets": input_sets,
        "output_sets": output_sets,
    }

    try:
        response = requests.post(
            f"{BASE_URL}/register/function",
            data=BSON.encode(payload),
            headers={"Content-Type": "application/octet-stream"},
            timeout=10,
        )
    except ConnectionError as exc:
        raise RuntimeError(
            "The server closed the connection during registration. "
            "Check the server terminal for a Rust panic or registration error."
        ) from exc
    response.raise_for_status()
    print("registered:", response.content.decode(errors="replace"))

def invoke_function(
    function_name: str,
    input_sets: list[dict],
    output_parser: Optional[Callable] = None,
) -> dict:
    payload = {
        "name": function_name,
        "sets": input_sets,
    }

    response = requests.post(
        f"{BASE_URL}/hot/{function_name}",
        data=BSON.encode(payload),
        headers={"Content-Type": "application/octet-stream"},
        timeout=10,
    )
    response.raise_for_status()

    decoded = BSON(response.content).decode()
    print("raw response:", decoded)

    if output_parser is not None:
        return output_parser(decoded)
    return decoded


def register_composition(composition: str) -> None:
    payload = {"composition": composition}
    response = requests.post(
        f"{BASE_URL}/register/composition",
        data=BSON.encode(payload),
        headers={"Content-Type": "application/octet-stream"},
        timeout=10,
    )
    response.raise_for_status()
    print("registered composition:", response.content.decode(errors="replace"))


def invoke_composition(
    composition_name: str,
    input_sets: list[dict],
    output_parser: Optional[Callable] = None,
) -> dict:
    payload = {
        "name": composition_name,
        "sets": input_sets,
    }

    response = requests.post(
        f"{BASE_URL}/hot/compute",
        data=BSON.encode(payload),
        headers={"Content-Type": "application/octet-stream"},
        timeout=10,
    )
    response.raise_for_status()

    decoded = BSON(response.content).decode()
    print("raw response:", decoded)

    if output_parser is not None:
        return output_parser(decoded)
    return decoded


