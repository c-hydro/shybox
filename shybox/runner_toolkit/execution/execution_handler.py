
# libraries
from __future__ import annotations

import os
import subprocess
import threading
import queue
import json
import pandas as pd
import datetime as dt

from typing import Optional, Tuple, List, Any, Dict
from tabulate import tabulate
from pathlib import Path

from shybox.logging_toolkit.lib_logging_utils import get_log
from shybox.generic_toolkit.lib_utils_string import convert_bytes2string
from shybox.runner_toolkit.execution.lib_utils_execution import (
    build_execution_collections,
    prepare_executable_from_library,
    should_skip_execution,
    load_execution_info,
    save_execution_info,
    prepare_ld_library_path,
    build_command,
    clean_stderr,
    normalize_path,
    check_library_path,
)

from shybox.default.lib_default_time import time_format_algorithm
from shybox.logging_toolkit.logging_handler import LoggingManager

_SCALARS = (str, int, float, bool, type(None))

# class ExecutionManager
class ExecutionManager:
    """
    Single, high-level manager for running an external application from a config.

    Responsibilities:
      - Parse execution_obj (description, executable, library, info, deps).
      - Apply {TAG} templating via settings_obj.
      - Prepare executable (copy from library, chmod/check).
      - Manage .info file: skip or rerun based on execution_update.
      - Set up environment (LD_LIBRARY_PATH from deps).
      - Build and run command-line (buffered or streaming).
      - Handle IEEE flags in stderr.
      - Save execution_info to .info and return it.

    Usage:
        manager = ExecutionManager(
            execution_obj=execution_obj,
            time_obj=time_obj,
            settings_obj={'RUN': 'exec_base'},
            execution_update=True,
            stream_output=True,
        )
        execution_info = manager.run()
    """

    def __init__(
        self,
        execution_obj: Dict[str, Any],
        time_obj: Optional[Dict[str, Any]] = None,
        settings_obj: Optional[Dict[str, Any]] = None,
        execution_update: bool = True,
        stream_output: bool = True,
        timeout: Optional[int] = None,
        logger: LoggingManager | None = None,
    ) -> None:

        # Set up logger for this ConfigManager instance
        self.log = LoggingManager.get_logger(
            logger=logger, name="ExecutionManager", set_as_current=False,
        )

        self.execution_obj = execution_obj
        self.time_obj = time_obj
        self.settings_obj = settings_obj or {}
        self.execution_update = execution_update
        self.stream_output = stream_output
        self.timeout = timeout

        # --- metadata from description ---
        desc = execution_obj.get("description", {})
        self.exec_name = desc.get("execution_name", "exec")
        self.exec_mode = desc.get("execution_mode", "default")

        # --- derive paths from config + templates ---
        collections = build_execution_collections(
            execution_obj=self.execution_obj,
            settings_obj=self.settings_obj,
        )

        self.file_exec: str = collections["executable"]
        self.file_library: str = collections["library"]
        self.file_info: str = collections["info"]
        self.deps: List[str] = [
            normalize_path(v)
            for k, v in collections.items()
            if k.startswith("deps_")
        ]

        self.exec_args = self.execution_obj["executable"]["arguments"]

        self.cwd = os.path.dirname(self.file_exec)
        self.env = os.environ.copy()

        self.command: List[str] = []

        # store validation checks from last run/dry_run
        self._checks: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    def _prepare_executable(self) -> None:
        """
        Copy executable from library (if needed) and ensure it's valid.
        """
        prepare_executable_from_library(
            file_exec=self.file_exec,
            file_library=self.file_library,
            execution_update=self.execution_update,
        )

    # ------------------------------------------------------------------
    def _prepare_environment_and_command(self) -> None:
        """
        Set env (LD_LIBRARY_PATH) and build full command.
        """
        # Check deps
        for d in self.deps:
            check_library_path(d)

        self.env = prepare_ld_library_path(self.env, self.deps)
        self.command = build_command(self.file_exec, self.exec_args)

    # ------------------------------------------------------------------
    def _run_buffer(
        self,
        log_tag: str,
    ) -> Tuple[Optional[str], Optional[str], int]:
        log = get_log()
        if hasattr(log, "info_up"):
            log.info_up("Run (buffered mode)", tag=log_tag)
        else:
            log.info("Run (buffered mode)")

        proc = subprocess.run(
            self.command,
            cwd=self.cwd,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout,
            check=False,
        )

        stdout = convert_bytes2string(proc.stdout) if proc.stdout else None
        stderr = convert_bytes2string(proc.stderr) if proc.stderr else None
        stderr = clean_stderr(stderr)

        msg_exit = f"Exit code: {proc.returncode}"
        if hasattr(log, "info_down"):
            log.info_down(msg_exit, tag=log_tag)
        else:
            log.info(msg_exit)

        return stdout, stderr, proc.returncode

    # ------------------------------------------------------------------

    def _run_stream(self, log_tag: str) -> Tuple[Optional[str], Optional[str], int]:
        log = get_log()
        (log.info_up if hasattr(log, "info_up") else log.info)("Run (streaming mode)", tag=log_tag)

        # Ensure unbuffered/line-buffered behavior where possible
        env = dict(self.env or {})
        env.setdefault("PYTHONUNBUFFERED", "1")  # harmless if not python

        proc = subprocess.Popen(
            self.command,
            cwd=self.cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # IMPORTANT: enables real line buffering
            bufsize=1,  # line buffered (only meaningful with text=True)
            universal_newlines=True,
            errors="replace",
        )

        q: "queue.Queue[tuple[str, str]]" = queue.Queue()
        stdout_lines: List[str] = []
        stderr_lines: List[str] = []

        t_out = threading.Thread(target=_pump_lines, args=(proc.stdout, "stdout", q), daemon=True)
        t_err = threading.Thread(target=_pump_lines, args=(proc.stderr, "stderr", q), daemon=True)
        t_out.start()
        t_err.start()

        # Consume lines until process exits and both pumps are done
        while True:
            try:
                name, line = q.get(timeout=0.1)
            except queue.Empty:
                if proc.poll() is not None and not (t_out.is_alive() or t_err.is_alive()):
                    break
                continue

            line = line.rstrip("\n")
            if name == "stdout":
                stdout_lines.append(line)
                log.info(line)
            else:
                stderr_lines.append(line)
                log.warning(line)

        exit_code = proc.wait()

        stdout = "\n".join(stdout_lines) if stdout_lines else None
        stderr = "\n".join(stderr_lines) if stderr_lines else None
        stderr = clean_stderr(stderr)

        msg_exit = f"Exit code: {exit_code}"
        (log.info_down if hasattr(log, "info_down") else log.info)(msg_exit, tag=log_tag)

        return stdout, stderr, exit_code

    # ------------------------------------------------------------------
    def run(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Orchestrate the whole execution lifecycle and return execution_info.

        Parameters
        ----------
        dry_run : bool
            If True, perform configuration checks and command build,
            but DO NOT actually execute the external program.
        """
        log = get_log()
        log_tag = self.exec_name

        span = getattr(log, "span", None)
        if callable(span):
            with span(f"Run execution '{self.exec_name}'", tag=log_tag):
                return self._run_core(log_tag, dry_run=dry_run)
        else:
            log.info(f"Run execution '{self.exec_name}'")
            return self._run_core(log_tag, dry_run=dry_run)


    # ------------------------------------------------------------------
    def _run_core(self, log_tag: str, dry_run: bool = False) -> Dict[str, Any]:
        """
        Internal core used by run().

        - Performs checks and stores them into self._checks.
        - If dry_run=True, returns a report without executing the program.
        """

        log = get_log()

        # start a fresh checks dict
        self._checks = {
            "dry_run": dry_run,
            "deps_ok": True,
            "file_exec_exists": os.path.isfile(self.file_exec),
            "file_library_exists": os.path.isfile(self.file_library),
            "cwd_exists": os.path.isdir(self.cwd),
            "command_built": None,
            "errors": [],
        }

        # 1. Skip logic (only for real runs)
        if not dry_run and should_skip_execution(self.file_info, self.execution_update):
            log.info(f'Execution info exists: "{self.file_info}" → skip run')
            info = load_execution_info(self.file_info)

            # if older info had no checks, at least attach current quick checks
            info.setdefault("checks", self._checks)
            return info

        # 2. Prepare executable (copy / chmod / existence)
        try:
            self._prepare_executable()
        except Exception as e:
            msg = f"Executable preparation failed: {e}"
            log.warning(msg)
            self._checks["errors"].append(msg)
            self._checks["file_exec_exists"] = os.path.isfile(self.file_exec)
            if not dry_run:
                raise

        # 3. Prepare environment + command (with dep checks)
        deps_ok = True
        for d in self.deps:
            try:
                check_library_path(d)
            except Exception as e:
                msg = f"Dependency invalid: {d} → {e}"
                log.warning(msg)
                self._checks["errors"].append(msg)
                deps_ok = False
                if not dry_run:
                    raise

        self._checks["deps_ok"] = deps_ok

        try:
            self.env = prepare_ld_library_path(self.env, self.deps)
            self.command = build_command(self.file_exec, self.exec_args)
            self._checks["command_built"] = " ".join(self.command)
        except Exception as e:
            msg = f"Command build error: {e}"
            log.warning(msg)
            self._checks["errors"].append(msg)
            if not dry_run:
                raise

        # 4. DRY RUN: just return a report with checks and no execution
        if dry_run:
            log.info("[DRY-RUN] No execution performed. Returning check report.")
            execution_info: Dict[str, Any] = {
                "exec_tag": self.exec_name,
                "exec_mode": self.exec_mode,
                "exec_time": self.time_obj,
                "exec_response": [None, None, None],
                "execution_obj": self.execution_obj,
                "settings_obj": self.settings_obj,
                "checks": self._checks,
            }
            return execution_info

        # 5. Real execution ----------------------------------------------------
        if self.stream_output:
            stdout, stderr, exit_code = self._run_stream(log_tag)
        else:
            stdout, stderr, exit_code = self._run_buffer(log_tag)

        # stderr check (after IEEE cleanup)
        if stderr is not None:
            msg = f"Execution '{self.exec_name}' failed: {stderr}"
            log.error(msg)
            self._checks["errors"].append(msg)
            # you might want to save checks before raising
            execution_info = {
                "exec_tag": self.exec_name,
                "exec_mode": self.exec_mode,
                "exec_time": self.time_obj,
                "exec_response": [stdout, stderr, str(exit_code)],
                "execution_obj": self.execution_obj,
                "settings_obj": self.settings_obj,
                "checks": self._checks,
            }
            save_execution_info(self.file_info, execution_info)
            raise RuntimeError(stderr)

        # 6. Save .info with checks
        execution_info: Dict[str, Any] = {
            "exec_tag": self.exec_name,
            "exec_mode": self.exec_mode,
            "exec_time": self.time_obj,
            "exec_response": [stdout, stderr, str(exit_code)],
            "execution_obj": self.execution_obj,
            "settings_obj": self.settings_obj,
            "checks": self._checks,
        }

        save_execution_info(self.file_info, execution_info)
        return execution_info


    # ------------------------------------------------------------------ #
    # Utilities for view()
    def __flat_dict_key(
        self,
        data: Dict[str, Any],
        prefix: str = "",
        separator: str = ":",
    ) -> Dict[str, Any]:
        """
        Flatten a nested dict using <separator> in the key path.
        """
        flat: Dict[str, Any] = {}

        if not isinstance(data, dict):
            return flat

        for k, v in data.items():
            key = f"{prefix}{separator}{k}" if prefix else str(k)
            if isinstance(v, dict):
                flat.update(self.__flat_dict_key(v, key, separator=separator))
            elif isinstance(v, (list, tuple)):
                try:
                    flat[key] = ", ".join(map(str, v))
                except Exception:
                    flat[key] = repr(v)
            else:
                try:
                    flat[key] = v
                except Exception:
                    flat[key] = repr(v)

        return flat

    def as_dict(self) -> Dict[str, Any]:
        """
        Compact snapshot of the ExecutionManager configuration, including
        last validation checks (if any).
        """
        return {
            "description": {
                "execution_name": self.exec_name,
                "execution_mode": self.exec_mode,
            },
            "paths": {
                "file_exec": getattr(self, "file_exec", None),
                "file_library": getattr(self, "file_library", None),
                "file_info": getattr(self, "file_info", None),
                "cwd": getattr(self, "cwd", None),
                "deps": getattr(self, "deps", None),
            },
            "command": {
                "executable": getattr(self, "file_exec", None),
                "arguments": getattr(self, "exec_args", None),
                "command_built": getattr(self, "command", None),
            },
            "runtime": {
                "execution_update": getattr(self, "execution_update", None),
                "stream_output": getattr(self, "stream_output", None),
                "timeout": getattr(self, "timeout", None),
            },
            "context": {
                "settings_obj": getattr(self, "settings_obj", None),
                "time_obj": getattr(self, "time_obj", None),
            },
            "checks": self._checks,  # <--- here
        }

    # ------------------------------------------------------------------ #
    # View method
    def view(
        self,
        section: dict | str | None = None,
        table_variable: str = "key",
        table_values: str = "value",
        table_format: str = "psql",
        table_print: bool = True,
        separator: str = ":",
        table_name: str = "ExecutionManager",
    ) -> str:
        """
        View configuration-like content as a table.

        section:
            - None      -> use self.as_dict()
            - dict      -> display that dict
            - "checks"  -> display last validation checks
            - other str -> reserved; raises
        """

        # --- decide what to display ---
        if isinstance(section, dict):
            data = section

        elif section is None:
            data = self.as_dict()

        elif isinstance(section, str):
            if section == "checks":
                data = self._checks
                table_name = "ExecutionManager checks"
            else:
                raise ValueError(
                    "ExecutionManager.view(): unsupported section name. "
                    'Use section=None, "checks", or pass a dict.'
                )
        else:
            raise TypeError(
                "section must be None, a section name (str), or a dict, "
                f"not {type(section)}"
            )

        if not isinstance(data, dict):
            raise ValueError("view() expects a dict-like object to display.")

        # --- flatten dict ---
        flat = self.__flat_dict_key(data, separator=separator)

        # --- build DataFrame ---
        df = pd.DataFrame.from_dict(flat, orient="index", columns=[table_values])
        df.index.name = table_variable

        # --- create base table ---
        base = tabulate(
            df,
            headers=[table_variable, table_values],
            tablefmt=table_format,
            showindex=True,
            missingval="N/A",
        )

        lines = base.split("\n")

        # --- find first border line (e.g. "+-----+-----+") ---
        border_idx = None
        border_line = None
        for i, line in enumerate(lines):
            if line.startswith("+") and line.endswith("+"):
                border_idx = i
                border_line = line
                break

        if border_idx is None or border_line is None:
            title_line = f"view :: {table_name}"
            final = f"{title_line}\n{base}"
            if table_print:
                print(final)
            return final

        # --- full-width title row ---
        table_width = len(border_line)
        inner_width = table_width - 2

        title_text = f" view :: {table_name}"
        title_content = title_text.ljust(inner_width)[:inner_width]
        title_row = "|" + title_content + "|"

        # --- insert title row just after top border ---
        insert_pos = border_idx + 1
        lines.insert(insert_pos, title_row)
        lines.insert(insert_pos + 1, border_line)

        final_table = "\n".join(lines)
        final_table = "\n" + final_table + "\n"

        if table_print:
            print(final_table)

        return final_table

    # ------------------------------------------------------------------ #
    # Representation
    def __repr__(self) -> str:
        cmd_preview = None
        try:
            if getattr(self, "command", None):
                cmd_preview = " ".join(map(str, self.command))[:120]
        except Exception:
            cmd_preview = None

        return (
            "ExecutionManager("
            f"exec_name={self.exec_name!r}, "
            f"exec_mode={self.exec_mode!r}, "
            f"file_exec={getattr(self, 'file_exec', None)!r}, "
            f"file_library={getattr(self, 'file_library', None)!r}, "
            f"file_info={getattr(self, 'file_info', None)!r}, "
            f"deps={len(getattr(self, 'deps', []) or [])}, "
            f"args={getattr(self, 'exec_args', None)!r}, "
            f"cwd={getattr(self, 'cwd', None)!r}, "
            f"timeout={getattr(self, 'timeout', None)!r}, "
            f"stream_output={getattr(self, 'stream_output', None)!r}, "
            f"command={cmd_preview!r})"
        )

    def analyze(self, execution_info: Dict[str, Any]) -> "ExecutionAnalyzer":
        """
        Create an ExecutionAnalyzer bound to this manager.
        """
        return ExecutionAnalyzer(self, execution_info)


class ExecutionAnalyzer:
    """
    Analyzer for ExecutionManager results.

    Rules:
      - NO table rendering logic here.
      - ASCII rendering delegates to ExecutionManager.view_sections() (same style as manager.view()).
      - All dumping/writing methods (ascii/json/env_vars) are implemented in THIS class.
      - Accepts extra dict sections to append, and generic objects to coerce into dict sections.

    Instantiation pattern (recommended):
      - Add ExecutionManager.analyze(execution_info) -> ExecutionAnalyzer(self, execution_info)
      - Users don't pass manager explicitly.
    """

    def __init__(
        self,
        manager: Any,  # ExecutionManager injected by ExecutionManager.analyze(...)
        execution_info: Dict[str, Any],
        *,
        name: str = "ExecutionAnalyzer",
        logger: LoggingManager | None = None,
    ) -> None:

        # Set up logger for this ConfigManager instance
        self.log = LoggingManager.get_logger(
            logger=logger, name="ExecutionAnalyzer", set_as_current=False,
        )

        if execution_info is None or not isinstance(execution_info, dict):
            raise TypeError(f"{name}: execution_info must be a dict, not {type(execution_info)}")

        checks = execution_info.get("checks", {})
        self._checks: Dict[str, Any] = checks if isinstance(checks, dict) else {}

        resp = execution_info.get("exec_response", [None, None, None])
        if isinstance(resp, (list, tuple)) and len(resp) == 3:
            # get dry_run condition from checks
            dry_run = checks.get("dry_run", None)
            if dry_run is not None:
                if not dry_run:
                    execution_msg = ['SIMULATION COMPLETED - RUN OK [NO ERRORS]', None, None]
                    execution_info['exec_response'] = execution_msg
                else:
                    execution_msg = ['SIMULATION COMPLETED - DRY RUN [SETTINGS TEST]', None, None]
                    execution_info['exec_response'] = execution_msg
            else:
                execution_msg = ['SIMULATION COMPLETED - UNKNOWN RUN TYPE', None, None]
                execution_info['exec_response'] = execution_msg
            self._response = execution_msg
        else:
            self._response = resp

        # update information
        self.manager = manager
        self.execution_info = execution_info
        self.name = name

    # ------------------------------------------------------------------ #
    # Core getters / status
    @property
    def exec_tag(self) -> Optional[str]:
        v = self.execution_info.get("exec_tag", None)
        return str(v) if v is not None else None

    @property
    def exec_mode(self) -> Optional[str]:
        v = self.execution_info.get("exec_mode", None)
        return str(v) if v is not None else None

    @property
    def stdout(self) -> Optional[str]:
        v = self._response[0]
        return None if v is None else str(v)

    @property
    def stderr(self) -> Optional[str]:
        v = self._response[1]
        return None if v is None else str(v)

    @property
    def exit_code(self) -> Optional[int]:
        v = self._response[2]
        if v is None:
            return None
        try:
            return int(v)
        except Exception:
            return None

    @property
    def dry_run(self) -> bool:
        return bool(self._checks.get("dry_run", False))

    @property
    def command(self) -> Optional[str]:
        cmd = self._checks.get("command_built", None)
        if isinstance(cmd, str) and cmd.strip():
            return cmd
        return None

    @property
    def errors(self) -> List[str]:
        errs = self._checks.get("errors", [])
        if isinstance(errs, list):
            return [str(e) for e in errs]
        if errs:
            return [str(errs)]
        return []

    @property
    def ok(self) -> bool:
        if self.dry_run:
            return len(self.errors) == 0

        if self.stderr is not None and self.stderr.strip() != "":
            return False

        code = self.exit_code
        if code is not None and code != 0:
            return False

        return len(self.errors) == 0

    # ------------------------------------------------------------------ #
    # Section/object coercion
    def _coerce_to_dict(self, obj: Any) -> Dict[str, Any]:
        """
        Convert arbitrary objects to a dict section.

        Priority:
          - None -> {}
          - dict -> dict
          - pd.Timestamp / datetime / date -> {"value": iso-string}
          - pd.Timedelta / datetime.timedelta -> {"value": string}
          - has __getstate__() -> that (if dict)
          - has as_dict() -> that (if dict)
          - has __dict__ -> vars(obj)
          - else -> {"value": str(obj)}
        """
        if obj is None:
            return {}

        if isinstance(obj, dict):
            return obj

        # ---- time-like scalars -------------------------------------------------
        # pandas Timestamp
        if isinstance(obj, pd.Timestamp):
            if isinstance(time_format_algorithm, str) and time_format_algorithm:
                value = obj.strftime(time_format_algorithm)
            else:
                value = obj.isoformat()
            return {"value": value}
        # python datetime/date
        if isinstance(obj, (dt.datetime, dt.date)):
            if isinstance(time_format_algorithm, str) and time_format_algorithm:
                value = obj.strftime(time_format_algorithm)
            else:
                value = obj.isoformat()
            return {"value": value}

        # pandas Timedelta
        if isinstance(obj, pd.Timedelta):
            return {"value": str(obj)}

        # python timedelta
        if isinstance(obj, dt.timedelta):
            return {"value": str(obj)}

        # ---- default object hook (pickle-style) --------------------------------
        getstate = getattr(obj, "__getstate__", None)
        if callable(getstate):
            try:
                d = getstate()
                if isinstance(d, dict):
                    return d
            except Exception:
                pass

        # ---- custom export ------------------------------------------------------
        as_dict = getattr(obj, "as_dict", None)
        if callable(as_dict):
            try:
                d = as_dict()
                if isinstance(d, dict):
                    return d
            except Exception:
                pass

        # ---- plain object attributes -------------------------------------------
        try:
            d = vars(obj)
            if isinstance(d, dict):
                return d
        except Exception:
            pass

        return {"value": str(obj)}

    # ------------------------------------------------------------------ #
    # Data building

    def summary_dict(self) -> Dict[str, Any]:
        return {
            "exec_tag": self.exec_tag,
            "exec_mode": self.exec_mode,
            "dry_run": self.dry_run,
            "ok": self.ok,
            "exit_code": self.exit_code,
            "command": self.command,
            "n_errors": len(self.errors),
        }

    def as_dict(self, *, include_raw: bool = True) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "summary": self.summary_dict(),
            "checks": self._checks,
            "errors": {"errors": self.errors},
            "io": {"stdout": self.stdout, "stderr": self.stderr},
        }
        if include_raw:
            d["raw"] = self.execution_info
        return d

    def as_sections(
        self,
        *,
        extras: Optional[Dict[str, Dict[str, Any]]] = None,
        objects: Optional[Dict[str, Any]] = None,
        include_object_snapshot: bool = True,
        include_raw_result: bool = True,
        include_analyzer_blocks: bool = True,
        include_raw_in_analyzer: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build sections dict[str, dict] for rendering/writing.

        extras:
            Strict dict[str, dict] - already dict sections.

        objects:
            Flexible dict[str, Any] - values can be dict or any object.
            Non-dict values will be coerced to dict via _coerce_to_dict().

        include_object_snapshot:
            Add manager.as_dict() under 'object'.

        include_analyzer_blocks:
            Add 'summary', 'checks', 'errors', 'io' (and optionally 'raw').

        include_raw_result:
            Add the raw execution_info dict under 'result'.

        include_raw_in_analyzer:
            If True, includes 'raw' section inside analyzer blocks (can be large).
        """
        sections: Dict[str, Dict[str, Any]] = {}

        if include_object_snapshot:
            obj = self.manager.as_dict()
            if isinstance(obj, dict):
                sections["object"] = obj

        if include_analyzer_blocks:
            report = self.as_dict(include_raw=include_raw_in_analyzer)
            for k, v in report.items():
                if isinstance(v, dict):
                    sections[k] = v

        if include_raw_result:
            sections["result"] = self.execution_info

        if extras is not None:

            if isinstance(extras, dict):
                obj_map = extras
            else:
                try:
                    if hasattr(extras, "__getstate__"):
                        obj_map = extras.__getstate__()
                    else:
                        obj_map = vars(extras)  # fallback
                except TypeError:
                    self.log.error(
                        f"objects must be a dict or an object with __dict__, not {type(extras)}"
                    )

            for k, obj in obj_map.items():
                if not str(k) in sections.keys():
                    sections[str(k)] = self._coerce_to_dict(obj)
                else:
                    self.log.warning(
                        f"ExecutionAnalyzer.as_sections(): skipping object key '{k}' "
                        "because it already exists in sections."
                    )

        if objects is not None:
            if isinstance(objects, dict):
                obj_map = objects
            else:
                try:
                    if hasattr(objects, "__getstate__"):
                        obj_map = objects.__getstate__()
                    else:
                        obj_map = vars(objects)  # fallback
                except TypeError:
                    self.log.error(
                        f"objects must be a dict or an object with __dict__, not {type(objects)}"
                    )

            for k, obj in obj_map.items():
                if obj is None:
                    continue
                if not str(k) in sections.keys():
                    sections[str(k)] = self._coerce_to_dict(obj)
                else:
                    self.log.warning(
                        f"ExecutionAnalyzer.as_sections(): skipping object key '{k}' "
                        "because it already exists in sections."
                    )

        return sections

    # ------------------------------------------------------------------ #
    # View (delegates to ExecutionManager ONLY for ascii rendering)

    def view(
        self,
        *,
        extras: Optional[Dict[str, Dict[str, Any]]] = None,
        objects: Optional[Dict[str, Any]] = None,
        include_object_snapshot: bool = True,
        include_raw_result: bool = True,
        include_analyzer_blocks: bool = True,
        include_raw_in_analyzer: bool = False,
        table_print: bool = True,
        table_name: str = "ExecutionAnalyzer",
        **view_kwargs: Any,
    ) -> str:
        sections = self.as_sections(
            extras=extras,
            objects=objects,
            include_object_snapshot=include_object_snapshot,
            include_raw_result=include_raw_result,
            include_analyzer_blocks=include_analyzer_blocks,
            include_raw_in_analyzer=include_raw_in_analyzer,
        )

        return self.manager.view(
            sections,
            table_print=table_print,
            table_name=table_name,
            **view_kwargs,
        )

    def view_summary(
        self,
        *,
        extras: Optional[Dict[str, Dict[str, Any]]] = None,
        objects: Optional[Dict[str, Any]] = None,
        table_print: bool = True,
        table_name: str = "ExecutionAnalyzer",
        **view_kwargs: Any,
    ) -> str:
        sections: Dict[str, Dict[str, Any]] = {"summary": self.summary_dict()}

        if extras:
            for k, v in extras.items():
                if isinstance(v, dict):
                    sections[str(k)] = v

        if objects:
            for k, obj in objects.items():
                if obj is None:
                    continue
                sections[str(k)] = self._coerce_to_dict(obj)

        return self.manager.view_sections(
            sections,
            table_print=table_print,
            table_name=table_name,
            **view_kwargs,
        )

    # ------------------------------------------------------------------ #
    # Dump helpers (strings)

    def dump_ascii(self, **kwargs: Any) -> str:
        """
        Same args as view(), but forces table_print=False.
        """
        kwargs.setdefault("table_print", False)
        return self.view(**kwargs)

    def dump_json(
        self,
        *,
        extras: Optional[Dict[str, Dict[str, Any]]] = None,
        objects: Optional[Dict[str, Any]] = None,
        include_object_snapshot: bool = True,
        include_raw_result: bool = True,
        include_analyzer_blocks: bool = True,
        include_raw_in_analyzer: bool = False,
        indent: int = 2,
    ) -> str:
        sections = self.as_sections(
            extras=extras,
            objects=objects,
            include_object_snapshot=include_object_snapshot,
            include_raw_result=include_raw_result,
            include_analyzer_blocks=include_analyzer_blocks,
            include_raw_in_analyzer=include_raw_in_analyzer,
        )
        return json.dumps(sections, indent=indent, default=str, ensure_ascii=False)

    # ---------------- env var helpers (self-contained)

    def _env_key(self, s: str) -> str:
        out = []
        for ch in str(s):
            out.append(ch.upper() if ch.isalnum() else "_")
        key = "".join(out)
        while "__" in key:
            key = key.replace("__", "_")
        return key.strip("_") or "KEY"

    def _env_quote(self, v: Any) -> str:
        if v is None:
            return '""'
        if isinstance(v, bool):
            return '"1"' if v else '"0"'
        if isinstance(v, (int, float)):
            return f'"{v}"'

        if isinstance(v, (list, tuple)):
            try:
                s = " ".join(map(str, v))
            except Exception:
                s = repr(v)
        else:
            try:
                s = str(v)
            except Exception:
                s = repr(v)

        s = s.replace("\\", "\\\\").replace('"', '\\"')
        s = s.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
        return f'"{s}"'

    def dump_env_vars(
        self,
        *,
        extras: Optional[Dict[str, Dict[str, Any]]] = None,
        objects: Optional[Dict[str, Any]] = None,
        include_object_snapshot: bool = True,
        include_raw_result: bool = True,
        include_analyzer_blocks: bool = True,
        include_raw_in_analyzer: bool = False,
        prefix: str = "EXECUTION",
        separator: str = ":",
        include_section: bool = True,
        sort_keys: bool = True,
    ) -> str:
        sections = self.as_sections(
            extras=extras,
            objects=objects,
            include_object_snapshot=include_object_snapshot,
            include_raw_result=include_raw_result,
            include_analyzer_blocks=include_analyzer_blocks,
            include_raw_in_analyzer=include_raw_in_analyzer,
        )

        # Use manager's flatten for consistency (name-mangled private method)
        flatten = getattr(self.manager, "_ExecutionManager__flat_dict_key")

        lines: List[str] = []
        pfx = self._env_key(prefix)

        for sec_name, sec_data in sections.items():
            if not isinstance(sec_data, dict):
                continue

            flat = flatten(sec_data, separator=separator)
            items = list(flat.items())
            if sort_keys:
                items.sort(key=lambda x: str(x[0]))

            for k, v in items:
                parts = [pfx]
                if include_section:
                    parts.append(self._env_key(sec_name))
                parts.append(self._env_key(str(k).replace(separator, "_")))
                env_key = "_".join(parts)
                lines.append(f"{env_key}={self._env_quote(v)}")

        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ #
    # Write helpers (files)

    def write_to_ascii(self, path: str, **kwargs: Any) -> str:
        content = self.dump_ascii(**kwargs)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return content

    def write_to_json(self, path: str, *, indent: int = 2, **kwargs: Any) -> str:
        content = self.dump_json(indent=indent, **kwargs)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return content

    def write_to_env_vars(self, path: str, **kwargs: Any) -> str:
        content = self.dump_env_vars(**kwargs)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return content

    def write_to(
        self,
        path: Optional[str] = None,
        *,
        file_name: Optional[str] = None,
        fmt: str = "env",
        prefix: str = "HMC",
        **kwargs: Any,
    ) -> str:
        fmt_norm = (fmt or "").lower().strip()

        # Canonical format + default filename + expected extension
        if fmt_norm in ("ascii", "txt", "text", "table"):
            fmt_kind = "ascii"
            default_name = "info.txt"
            expected_ext = ".txt"
        elif fmt_norm == "json":
            fmt_kind = "json"
            default_name = "info.json"
            expected_ext = ".json"
        elif fmt_norm in ("env", "env_vars", "dotenv"):
            fmt_kind = "env"
            default_name = "info.env"
            expected_ext = ".env"
        else:
            self.log.error(f"ExecutionAnalyzer.write_to(): unsupported fmt '{fmt}'.")
            raise ValueError(f"Unsupported fmt '{fmt}'")

        file_name_provided = file_name is not None

        # Base fallback: folder of the running app
        try:
            app_dir = Path(__file__).resolve().parent
        except NameError:
            app_dir = Path.cwd()

        # Resolve destination directory + file name
        dest_dir: Path
        fname: Optional[str] = file_name.strip() if file_name and file_name.strip() else None

        if path is None:
            dest_dir = app_dir
        else:
            p = Path(path)

            # If path looks like a file and no file_name override → split
            if p.suffix and not file_name_provided:
                dest_dir = p.parent
                fname = p.name
            else:
                dest_dir = p

            # Try to create directory + writability check
            try:
                dest_dir.mkdir(parents=True, exist_ok=True)
                test_file = dest_dir / ".write_check"
                test_file.touch(exist_ok=True)
                test_file.unlink()
            except Exception:
                dest_dir = app_dir
                self.log.warning(
                    "write_to(): info path is not writable; using application directory instead."
                )

        # Default filename (format-related) if still missing
        if not fname:
            fname = default_name

        # Extension check (+ add extension if missing)
        name_path = Path(fname)
        current_ext = name_path.suffix.lower()

        if not current_ext:
            # No extension at all -> add expected one quietly
            fname = str(name_path.with_suffix(expected_ext))
        elif current_ext != expected_ext:
            self.log.warning(
                f"write_to(): file extension '{current_ext}' does not match fmt '{fmt_kind}' "
                f"(expected '{expected_ext}'). Using '{fname}' as provided."
            )

        dest = dest_dir / fname
        dest_str = str(dest)

        # Dispatch by format
        if fmt_kind == "ascii":
            return self.write_to_ascii(dest_str, **kwargs)

        if fmt_kind == "json":
            return self.write_to_json(dest_str, **kwargs)

        # fmt_kind == "env"
        env_prefix = (prefix or "").strip()
        if env_prefix and not env_prefix.endswith("_"):
            env_prefix += "_"

        return self.write_to_env_vars(
            dest_str,
            prefix=env_prefix,
            **kwargs,
        )

# helpers
def _pump_lines(stream, name: str, q: "queue.Queue[tuple[str, str]]"):
    try:
        for line in stream:
            q.put((name, line))
    finally:
        try:
            stream.close()
        except Exception:
            pass

