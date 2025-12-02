# shybox/runner_toolkit/execution/execution_handler.py

import os
import subprocess
from typing import Optional, Tuple, List, Any, Dict

from shybox.logging_toolkit.lib_logging_utils import get_log

from shybox.generic_toolkit.lib_utils_string import convert_bytes2string

from .lib_utils_execution import (
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
    def run(self) -> Dict[str, Any]:
        """
        Orchestrate the whole execution lifecycle and return execution_info.
        Handles .info skip logic + writing.
        """
        log = get_log()
        log_tag = self.exec_name

        # Use span if available
        span = getattr(log, "span", None)
        if callable(span):
            with span(f"Run execution '{self.exec_name}'", tag=log_tag):
                return self._run_core(log_tag)
        else:
            log.info(f"Run execution '{self.exec_name}'")
            return self._run_core(log_tag)

    # ------------------------------------------------------------------
    def _run_core(self, log_tag: str) -> Dict[str, Any]:
        """
        Internal core used by run(). Split out to reuse with/without span.
        """

        # 1. Skip logic based on .info and execution_update
        if should_skip_execution(self.file_info, self.execution_update):
            log = get_log()
            log.info(f'Execution info exists: "{self.file_info}" → skip run')
            return load_execution_info(self.file_info)

        # 2. Prepare executable (copy from library, chmod/existence)
        self._prepare_executable()

        # 3. Prepare environment + command
        self._prepare_environment_and_command()

        # 4. Run process
        if self.stream_output:
            stdout, stderr, exit_code = self._run_stream(log_tag)
        else:
            stdout, stderr, exit_code = self._run_buffer(log_tag)

        # 5. stderr check (after IEEE cleanup)
        if stderr is not None:
            log = get_log()
            log.error(f"Execution '{self.exec_name}' failed: {stderr}")
            raise RuntimeError(stderr)

        # 6. Build execution_info and save to .info
        execution_info: Dict[str, Any] = {
            "exec_tag": self.exec_name,
            "exec_mode": self.exec_mode,
            "exec_time": self.time_obj,
            "exec_response": [stdout, stderr, str(exit_code)],
            "execution_obj": self.execution_obj,
            "settings_obj": self.settings_obj,
        }

        save_execution_info(self.file_info, execution_info)
        return execution_info
