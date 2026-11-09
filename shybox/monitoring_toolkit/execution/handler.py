"""Execution lifecycle tracking, independent of Flask and scheduler libraries."""
import os
import socket
import threading
import time
import uuid
from pathlib import Path

from ..common.io import SCHEMA_VERSION, STATUSES, time_string, utc_now, write_status


class RunMonitor:
    """One writer per attempt. Use directly, as a context manager, or from a worker.

    UTC timestamps are used throughout; naive model initialization times are
    interpreted as UTC. Monitoring errors raise here; runner integration logs
    them without replacing application results.
    """
    def __init__(self, folder_name, *, run_name, application, run_time=None,
                 run_domain=None, run_mode=None, ensemble=None, task_id=None,
                 heartbeat_seconds=30, outputs=None):
        if heartbeat_seconds <= 0:
            raise ValueError('heartbeat_seconds must be positive.')
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.execution_id = uuid.uuid4().hex
        self.file_path = Path(folder_name) / f'{self.execution_id}.json'
        self.outputs = [str(Path(path).resolve()) for path in (outputs or [])]
        self.record = {
            'schema_version': SCHEMA_VERSION, 'execution_id': self.execution_id,
            'run_name': str(run_name), 'application': str(application),
            'run_time': time_string(run_time), 'run_domain': run_domain,
            'run_mode': run_mode, 'ensemble': ensemble, 'task_id': task_id,
            'host': socket.gethostname(), 'pid': os.getpid(),
            'status': 'running', 'started_at': None, 'updated_at': None,
            'finished_at': None, 'exit_code': None, 'duration_seconds': None,
            'error': None, 'outputs': [], 'output_status': 'unknown',
        }
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._started = None
        self._finished = False
        self.heartbeat_error = None

    def start(self):
        with self._lock:
            if self._started is not None or self._finished:
                raise RuntimeError('An execution attempt can only start once.')
            self._started = time.monotonic()
            self.record['started_at'] = time_string(utc_now())
            self._write()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        return self

    def _write(self):
        self.record['updated_at'] = time_string(utc_now())
        if self._started is not None:
            self.record['duration_seconds'] = round(time.monotonic() - self._started, 3)
        return write_status(self.file_path, self.record)

    def heartbeat(self):
        with self._lock:
            if not self._finished:
                self._write()

    def _heartbeat_loop(self):
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self.heartbeat()
            except Exception as exc:
                self.heartbeat_error = str(exc)

    def finish(self, exit_code=None, *, status=None, error=None):
        """A missing exit code cannot imply successful completion."""
        status = status or ('succeeded' if exit_code == 0 else 'failed')
        if status not in STATUSES - {'running'}:
            raise ValueError('Invalid final monitoring status.')
        if status == 'succeeded' and exit_code != 0:
            raise ValueError('Successful execution requires exit code zero.')
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        with self._lock:
            if self._finished:
                return str(self.file_path)
            self.record.update(status=status, exit_code=exit_code, error=str(error)[:4000] if error else None,
                               finished_at=time_string(utc_now()))
            self.record['outputs'] = [{'path': path, 'available': Path(path).is_file()} for path in self.outputs]
            available = [item['available'] for item in self.record['outputs']]
            self.record['output_status'] = ('available' if all(available) else 'missing') if available else 'unknown'
            self._write()
            self._finished = True
        return str(self.file_path)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, traceback):
        self.finish(0 if exc is None else None,
                    status='cancelled' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else None,
                    error=exc)
        return False
