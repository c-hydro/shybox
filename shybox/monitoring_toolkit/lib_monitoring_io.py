"""Portable, atomic JSON records for SHYBOX execution monitoring."""
import json
import math
import os
from pathlib import Path
import tempfile
from datetime import datetime, timezone

SCHEMA_VERSION = 1
STATUSES = {'running', 'succeeded', 'failed', 'cancelled', 'skipped', 'dry_run'}


def utc_now():
    return datetime.now(timezone.utc)


def time_string(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime):
        raise ValueError('Monitoring timestamps must be datetime values or ISO strings.')
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def write_status(file_path, record):
    """Replace one attempt's record; readers always see a complete JSON object."""
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent,
                                         prefix='.' + path.name, suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(record, stream, indent=2, allow_nan=False)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return str(path)


def read_status(file_path):
    record = json.loads(Path(file_path).read_text(encoding='utf-8'))
    if not isinstance(record, dict) or record.get('schema_version') != SCHEMA_VERSION:
        raise ValueError('Unsupported monitoring schema.')
    if record.get('status') not in STATUSES or not record.get('execution_id'):
        raise ValueError('Invalid monitoring identity or status.')
    if any(not isinstance(record.get(key), str) or not record[key] for key in ('run_name', 'application')):
        raise ValueError('Monitoring record requires run_name and application.')
    code = record.get('exit_code')
    if code is not None and (not isinstance(code, int) or isinstance(code, bool)):
        raise ValueError('exit_code must be an integer or null.')
    duration = record.get('duration_seconds')
    if duration is not None and (not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0):
        raise ValueError('Invalid monitoring duration.')
    if not isinstance(record.get('outputs', []), list) or any(
            not isinstance(item, dict) or not isinstance(item.get('path'), str) or
            not isinstance(item.get('available'), bool) for item in record.get('outputs', [])):
        raise ValueError('Invalid monitoring outputs.')
    for key in ('run_time', 'started_at', 'updated_at', 'finished_at'):
        if record.get(key) is not None:
            record[key] = time_string(record[key])
    if not record.get('updated_at'):
        raise ValueError('Monitoring record requires updated_at.')
    return record
