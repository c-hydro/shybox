from pathlib import Path

try:
    import tomllib  # Python >= 3.11
except ModuleNotFoundError:
    import tomli as tomllib  # Python <= 3.10

_METADATA_NA = "N/A"

def _read_pyproject():
    # shybox/__init__.py -> repo root is 1 level above shybox/
    root = Path(__file__).resolve().parents[1]
    pyproject_file = root / "pyproject.toml"
    if not pyproject_file.exists():
        return {}
    return tomllib.loads(pyproject_file.read_text())

# get package configuration
_cfg = _read_pyproject()

_project = _cfg.get("project", {})
_tool = _cfg.get("tool", {}).get("shybox", {})
_urls = _project.get("urls", {})

# package information
__name_project__ = _project.get("name", _METADATA_NA)
__version__ = _project.get("version", _METADATA_NA)
__description__ = _project.get("description", _METADATA_NA)
__build_date__ = _tool.get("build_date", _METADATA_NA)

# python requirements
__requires_python__ = _project.get("requires-python", _METADATA_NA)

# authors information
__authors__ = _project.get("authors", [])
__author_names__ = [author.get("name") for author in __authors__ if author.get("name")]
__author__ = (", ".join(__author_names__) if __author_names__ else _METADATA_NA)

# maintainers information
__maintainers__ = _project.get("maintainers", [])
__maintainer_names__ = [maintainer.get("name") for maintainer in __maintainers__ if maintainer.get("name")]
__maintainer__ = (", ".join(__maintainer_names__) if __maintainer_names__ else _METADATA_NA)

# project urls
__repository__ = _urls.get("Repository", _METADATA_NA)
__homepage__ = _urls.get("Homepage", _METADATA_NA)
__documentation__ = _urls.get("Documentation", _METADATA_NA)
__issues__ = _urls.get("Issues", _METADATA_NA)

# expose common decorators
from shybox.logging_toolkit.lib_logging_utils import with_logger
from shybox.generic_toolkit.lib_utils_code import deprecated
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import as_process

# info
__all__ = [
    "__name_project__",
    "__version__",
    "__description__",
    "__build_date__",
    "__requires_python__",

    "__authors__",
    "__author_names__",
    "__author__",

    "__maintainers__",
    "__maintainer_names__",
    "__maintainer__",

    "__repository__",
    "__homepage__",
    "__documentation__",
    "__issues__",

    "with_logger",
    "deprecated",
    "as_process",
]