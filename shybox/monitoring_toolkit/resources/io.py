"""Read resource snapshots without importing psutil or scientific collectors."""
import json
from pathlib import Path
from datetime import datetime
from ..common.io import utc_now


def read_memory_snapshot(file_name, stale_seconds=180):
    if not file_name or not Path(file_name).is_file():
        return {'status': 'missing', 'targets': [], 'host': {}, 'ruler': {}}
    def invalid(value):
        raise ValueError(f'Nonfinite JSON value: {value}')
    result = json.loads(Path(file_name).read_text(), parse_constant=invalid)
    if result.get('schema_version') != 1 or result.get('resource') != 'memory':
        raise ValueError('Unsupported memory snapshot.')
    updated = datetime.fromisoformat(result['generated_at'].replace('Z', '+00:00'))
    result['status'] = 'stale' if (utc_now()-updated).total_seconds() > stale_seconds else 'available'
    return result
