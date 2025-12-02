# shybox/runner_toolkit/execution/lib_utils_execution.py

import os
from typing import Dict, List, Tuple, Any

from shybox.logging_toolkit.lib_logging_utils import get_log  # LoggingManager-aware

from shybox.generic_toolkit.lib_utils_dict import create_dict_from_list
from shybox.generic_toolkit.lib_utils_string import (
    replace_string, fill_tags2string, convert_bytes2string,
)
from shybox.generic_toolkit.lib_utils_file import (
    split_file_path, join_file_path, sanitize_file_path,
    expand_file_path, copy_file,
)
from shybox.generic_toolkit.lib_utils_debug import (
    read_workspace_obj, write_workspace_obj,
)

# -------------------------------------------------------------------------
# small logging helpers (safe if LoggingManager not active)
# -------------------------------------------------------------------------
def _log_info(msg: str, tag: str | None = None, up: bool = False, down: bool = False):
    log = get_log()
    if up and hasattr(log, "info_up"):
        log.info_up(msg, tag=tag)
    elif down and hasattr(log, "info_down"):
        log.info_down(msg, tag=tag)
    else:
        if tag and hasattr(log, "info"):
            log.info(f"[{tag}] {msg}")
        else:
            log.info(msg)


def _log_warning(msg: str, tag: str | None = None):
    log = get_log()
    if hasattr(log, "warning"):
        if tag:
            log.warning(f"[{tag}] {msg}")
        else:
            log.warning(msg)
    else:
        print(f"WARNING: {msg}")


def _log_error(msg: str, tag: str | None = None):
    log = get_log()
    if hasattr(log, "error"):
        if tag:
            log.error(f"[{tag}] {msg}")
        else:
            log.error(msg)
    else:
        print(f"ERROR: {msg}")


# -------------------------------------------------------------------------
# PATH HELPERS
# -------------------------------------------------------------------------
def normalize_path(path: str) -> str:
    if not isinstance(path, str):
        return path
    p = expand_file_path(path)
    p = sanitize_file_path(p)
    return os.path.abspath(p)


def split_join_path(path: str) -> Tuple[str, str, str]:
    file_name, folder_name = split_file_path(path)
    full = join_file_path(folder_name=folder_name, file_name=file_name)
    return folder_name, file_name, full


# -------------------------------------------------------------------------
# EXECUTABLE / LIBRARY CHECKS
# -------------------------------------------------------------------------
def check_executable(path: str):
    if not os.path.exists(path):
        _log_error(f'Executable "{path}" not found', tag="exec_utils")
        raise RuntimeError(f'Executable "{path}" not found')

    if not os.path.isfile(path):
        _log_error(f'"{path}" is not a regular file', tag="exec_utils")
        raise RuntimeError(f'"{path}" must be a regular file')

    if not os.access(path, os.X_OK):
        _log_error(f'"{path}" is not executable', tag="exec_utils")
        raise RuntimeError(f'"{path}" is not executable')

    _log_info(f'Executable "{path}" ... OK', tag="exec_utils")


def check_library_path(path: str):
    if not os.path.exists(path):
        _log_error(f'Library dependency "{path}" not found', tag="exec_utils")
        raise RuntimeError(f'Library "{path}" not found')

    _log_info(f'Library "{path}" ... OK', tag="exec_utils")


# -------------------------------------------------------------------------
# LD_LIBRARY_PATH
# -------------------------------------------------------------------------
def prepare_ld_library_path(env: dict, deps: List[str]) -> dict:
    dirs = [d for d in deps if os.path.isdir(d)]
    if not dirs:
        return env

    old = env.get("LD_LIBRARY_PATH", "")
    parts = [p for p in old.split(":") if p]

    for d in dirs:
        if d not in parts:
            parts.append(d)

    env["LD_LIBRARY_PATH"] = ":".join(parts)
    _log_info(f'Set LD_LIBRARY_PATH="{env["LD_LIBRARY_PATH"]}"', tag="exec_utils")
    return env


# -------------------------------------------------------------------------
# COMMAND BUILDING
# -------------------------------------------------------------------------
def build_command(exec_app: str, exec_args: Any) -> List[str]:
    import shlex

    cmd = [exec_app]
    if isinstance(exec_args, str) and exec_args.strip():
        cmd.extend(shlex.split(exec_args))
    elif isinstance(exec_args, list):
        cmd.extend([str(a) for a in exec_args])

    _log_info(f'Command = "{" ".join(cmd)}"', tag="exec_utils")
    return cmd


# -------------------------------------------------------------------------
# STDERR CLEANUP (IEEE FLAGS)
# -------------------------------------------------------------------------
VALID_STDERR_FLAGS = [
    "IEEE_INVALID_FLAG",
    "IEEE_OVERFLOW_FLAG",
    "IEEE_UNDERFLOW_FLAG",
]

def clean_stderr(stderr: str | None) -> str | None:
    if stderr is None:
        return None

    for flag in VALID_STDERR_FLAGS:
        if flag in stderr:
            _log_warning(
                f'StdErr contains acceptable flag "{flag}" → ignored',
                tag="exec_utils",
            )
            return None

    return stderr


def to_text_or_none(b: bytes | None) -> str | None:
    if not b:
        return None
    return convert_bytes2string(b)


# -------------------------------------------------------------------------
# Build execution paths from config + templates
# -------------------------------------------------------------------------
def build_execution_collections(
    execution_obj: Dict[str, Any],
    settings_obj: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Build paths for:
      - executable
      - library
      - info
      - deps_{i}
    Apply {TAG} templating using settings_obj like {'RUN': 'exec_base'}.
    """
    settings_obj = settings_obj or {}

    exec_location = execution_obj["executable"]["location"]
    lib_location = execution_obj["library"]["location"]
    info_location = execution_obj.get("info", {}).get("location")

    if info_location is None:
        folder_exec, _, _ = split_join_path(exec_location)
        info_location = os.path.join(folder_exec, "execution.info")

    _, _, exec_path = split_join_path(exec_location)
    _, _, lib_path = split_join_path(lib_location)
    _, _, info_path = split_join_path(info_location)

    deps = execution_obj["library"].get("dependencies", [])
    deps_dict = create_dict_from_list(default_key="deps_{:}", list_values=deps)

    collections: Dict[str, Any] = {
        "executable": exec_path,
        "library": lib_path,
        "info": info_path,
        **deps_dict,
    }

    # Apply {TAG} templates using settings_obj
    for key, value in list(collections.items()):
        if isinstance(value, str) and "{" in value:
            for s_key, s_val in settings_obj.items():
                if s_key in value:
                    value = fill_tags2string(
                        value,
                        tags_format={s_key: "string"},
                        tags_filling={s_key: s_val},
                    )[0]
            collections[key] = value

    # Normalize all paths
    for k, v in list(collections.items()):
        if isinstance(v, str):
            collections[k] = normalize_path(v)

    _log_info("Execution paths:", tag="exec_utils")
    for k, v in collections.items():
        _log_info(f"  - {k}: {v}", tag="exec_utils")

    return collections


# -------------------------------------------------------------------------
# Prepare executable from library (copy & check)
# -------------------------------------------------------------------------
def prepare_executable_from_library(
    file_exec: str, file_library: str, execution_update: bool = True
) -> None:
    """
    - Optionally delete existing exec when execution_update is True
    - Ensure folder exists
    - Copy from library if needed
    - Check executable is valid
    """
    _log_info("Prepare executable", tag="exec_prepare", up=True)

    folder_exec, _, _ = split_join_path(file_exec)
    if execution_update and os.path.exists(file_exec):
        os.remove(file_exec)

    os.makedirs(folder_exec, exist_ok=True)

    if not os.path.exists(file_exec):
        if not os.path.exists(file_library):
            _log_error(
                f'Library executable "{file_library}" not found',
                tag="exec_prepare",
            )
            raise RuntimeError("Library file missing for executable creation")
        copy_file(file_library, file_exec)

    check_executable(file_exec)

    _log_info("Executable ready", tag="exec_prepare", down=True)


# -------------------------------------------------------------------------
# .info handling
# -------------------------------------------------------------------------
def should_skip_execution(info_location: str, execution_update: bool) -> bool:
    """
    - If execution_update is True → always run (remove existing info if present).
    - If False → skip when info exists.
    """
    if execution_update:
        if os.path.exists(info_location):
            os.remove(info_location)
        return False
    return os.path.exists(info_location)


def load_execution_info(info_location: str):
    return read_workspace_obj(info_location)


def save_execution_info(info_location: str, payload: dict):
    folder, _, _ = split_join_path(info_location)
    os.makedirs(folder, exist_ok=True)
    write_workspace_obj(info_location, payload)
    return payload
