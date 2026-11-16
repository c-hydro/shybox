"""Compare existing files with the hashes captured before this folder was created."""
import hashlib
import json
from pathlib import Path


def changed_originals():
    folder = Path(__file__).resolve().parent
    root = folder.parents[2]
    baseline = json.loads((folder / 'original_files.sha256.json').read_text())
    changes = []
    for name, expected in baseline.items():
        path = root / name
        actual = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        if actual != expected:
            changes.append(name)
    return changes


if __name__ == '__main__':
    changes = changed_originals()
    print(json.dumps({'original_files_unchanged': not changes, 'changed_files': changes}, indent=2))
    raise SystemExit(bool(changes))
