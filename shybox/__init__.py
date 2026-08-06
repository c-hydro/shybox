from pathlib import Path

try:
    import tomllib  # Python >= 3.11
except ModuleNotFoundError:
    import tomli as tomllib  # Python <= 3.10


def _read_pyproject():
    # shybox/__init__.py -> repo root is 1 level above shybox/
    root = Path(__file__).resolve().parents[1]
    pyproject_file = root / "pyproject.toml"

    if not pyproject_file.exists():
        return {}

    return tomllib.loads(pyproject_file.read_text())


_cfg = _read_pyproject()

__version__ = _cfg.get("project", {}).get("version", "0.0.0")
__build_date__ = _cfg.get("tool", {}).get("shybox", {}).get("build_date", None)
