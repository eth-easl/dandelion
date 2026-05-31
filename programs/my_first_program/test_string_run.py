import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dandelion_client import register_function, invoke_function

register_function(
    function_name='test_string',
    function_path='/tmp/dandelion_minimal/test_string',
    context_size=0x802_0000,
    engine_type='Kvm',
    input_sets=[['Input', None]],
    output_sets=['Output'],
)

result = invoke_function(
    function_name='test_string',
    input_sets=[{
        'identifier': '',
        'items': [{
            'identifier': '',
            'key': 0,
            'data': b'Hello'
        }]
    }],
    output_parser=None
)
print('Result:', result)
