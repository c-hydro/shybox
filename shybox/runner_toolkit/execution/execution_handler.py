
# libraries
import os
import subprocess
import pandas as pd

from typing import Optional, Tuple, List, Any, Dict
from tabulate import tabulate

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
    ) -> None:

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
    def _run_stream(
        self,
        log_tag: str,
    ) -> Tuple[Optional[str], Optional[str], int]:
        log = get_log()
        if hasattr(log, "info_up"):
            log.info_up("Run (streaming mode)", tag=log_tag)
        else:
            log.info("Run (streaming mode)")

        proc = subprocess.Popen(
            self.command,
            cwd=self.cwd,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout_chunks: List[bytes] = []
        stderr_chunks: List[bytes] = []

        while True:
            if proc.poll() is not None:
                # drain remaining
                rem_out = proc.stdout.read() if proc.stdout else b""
                if rem_out:
                    stdout_chunks.append(rem_out)
                    log.info(convert_bytes2string(rem_out).rstrip())

                rem_err = proc.stderr.read() if proc.stderr else b""
                if rem_err:
                    stderr_chunks.append(rem_err)
                    log.warning(convert_bytes2string(rem_err).rstrip())
                break

            line = proc.stdout.readline() if proc.stdout else b""
            if line:
                stdout_chunks.append(line)
                log.info(convert_bytes2string(line).rstrip())

            err = proc.stderr.readline() if proc.stderr else b""
            if err:
                stderr_chunks.append(err)
                log.warning(convert_bytes2string(err).rstrip())

        exit_code = proc.returncode
        stdout = convert_bytes2string(b"".join(stdout_chunks)) if stdout_chunks else None
        stderr = convert_bytes2string(b"".join(stderr_chunks)) if stderr_chunks else None
        stderr = clean_stderr(stderr)

        msg_exit = f"Exit code: {exit_code}"
        if hasattr(log, "info_down"):
            log.info_down(msg_exit, tag=log_tag)
        else:
            log.info(msg_exit)

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
