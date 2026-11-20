"""
Class Features

Name:          monitoring_handler
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251120'
Version:       '1.6.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import re
import socket
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from shybox.dataset_toolkit.lib_dataset_parse import get_date_from_str, substitute_string
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.monitoring_toolkit.lib_monitoring_io import SCHEMA_VERSION, STATUSES, time_string, utc_now, write_status
# ----------------------------------------------------------------------------------------------------------------------


def _round_to_utc_hour(value, method):
    if isinstance(value, str):
        value = get_date_from_str(value)
    utc_time = datetime.fromisoformat(time_string(value))
    hour = utc_time.replace(minute=0, second=0, microsecond=0)
    if method == 'ceil' and (utc_time != hour or getattr(value, 'nanosecond', 0)):
        hour += timedelta(hours=1)
    return hour


def _fill_path(template, tags, time_format):
    template = os.fspath(template)
    if time_format:
        template = re.sub(r'\{((?:placeholders\.)?(?:time_run|run_time))\}',
                          lambda match: f'{{{match[1]}:{time_format}}}', template)
    return substitute_string(template, tags)


def _path_tags(values, prefix=''):
    tags = {}
    for name, value in values.items():
        key = f'{prefix}{name}'
        if isinstance(value, dict):
            tags.update(_path_tags(value, f'{key}.'))
        else:
            tags[key] = value
    return tags


# ----------------------------------------------------------------------------------------------------------------------
# monitoring class
class 2:
    """One writer per attempt. Use directly, as a context manager, or from a worker.

    UTC timestamps are used throughout; naive model initialization times are
    interpreted as UTC. Monitoring errors raise here; runner integration logs
    them without replacing application results.

    ``path`` and ``file_name`` accept placeholders such as ``{model}``,
    ``{time_run:%Y%m%d}``, and ``{execution_id}``. Pass additional values in
    ``placeholders``. Unresolved placeholders are logged and kept in the path.
    Run times are floored to the UTC hour by default. Set ``time_rounding``
    to ``"ceil"`` to round up or ``None`` to keep the original precision.
    ``time_format`` controls time placeholders without an inline format.
    """
    def __init__(self, path, file_name, *, run_name, application, run_time=None,
                 run_domain=None, run_mode=None, ensemble=None, task_id=None,
                 time_rounding: str | None = 'floor', time_format: str | None = '%Y%m%d%H%M',
                 heartbeat_seconds=30, fail=True, outputs=None, placeholders: dict | None = None,
                 logger: LoggingManager | None = None, **kwargs):

        # Set up logger for this ConfigManager instance
        self.logger = LoggingManager.get_logger(logger=logger, name="TimeManager", set_as_current=False,)

        # check monitoring args
        if heartbeat_seconds <= 0:
            raise ValueError('heartbeat_seconds must be positive.')
        if not isinstance(fail, bool):
            raise ValueError('fail must be boolean.')
        if time_rounding not in (None, 'floor', 'flat', 'ceil'):
            raise ValueError('time_rounding must be "floor", "flat", "ceil", or None.')
        if time_format is not None and not isinstance(time_format, str):
            raise ValueError('time_format must be a string or None.')
        if run_time is None:
            run_time = datetime.now(timezone.utc)
        if time_rounding is not None:
            run_time = _round_to_utc_hour(run_time, time_rounding)

        self.heartbeat_seconds = float(heartbeat_seconds)
        self.fail = fail
        self.run_time = run_time

        self.execution_id = uuid.uuid4().hex
        run_time_iso = time_string(run_time)
        extra_tags = dict(placeholders or {})
        if time_rounding is not None:
            for key in ('time_run', 'run_time'):
                if extra_tags.get(key) is not None:
                    extra_tags[key] = time_string(_round_to_utc_hour(extra_tags[key], time_rounding))

        # configure tags (to use in filling or output information)
        tags = {
            **_path_tags(kwargs),
            'time_run': run_time_iso, 'run_time': run_time_iso, 'run_name': run_name,
            'application': application, 'app': application, 'model': application, 'tag': application,
            'run_domain': run_domain, 'run_mode': run_mode, 'ensemble': ensemble, 'task_id': task_id,
            'heartbeat_seconds': heartbeat_seconds, 'fail': fail,
            'time_rounding': time_rounding, 'time_format': time_format,
            **_path_tags(extra_tags),
            **_path_tags(extra_tags, 'placeholders.'),
            'execution_id': self.execution_id,
        }

        # solve monitoring path and file_name
        record_dir = _fill_path(path or '.', tags, time_format)
        record_name = f'{self.execution_id}.json' if file_name is None else _fill_path(file_name, tags, time_format)
        unresolved = set(re.findall(r'\{([\w.]+)(?::[^{}]*)?\}', f'{record_dir}/{record_name}'))
        if unresolved:
            self.logger.warning('Unresolved monitoring path placeholders: %s', ', '.join(sorted(unresolved)))
        self.file_path = Path(record_dir) / record_name
        self.outputs = [str(Path(path).resolve()) for path in (outputs or [])]

        self.record = {
            'schema_version': SCHEMA_VERSION, 'execution_id': self.execution_id,
            'run_name': str(run_name), 'application': str(application),
            'run_time': run_time_iso, 'run_domain': run_domain,
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

    # method to start monitoring
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

    # add record to set the updated_at time
    def _write(self):
        self.record['updated_at'] = time_string(utc_now())
        if self._started is not None:
            self.record['duration_seconds'] = round(time.monotonic() - self._started, 3)
        return write_status(self.file_path, self.record)

    # helper to check if process is running or finisched
    def heartbeat(self):
        with self._lock:
            if not self._finished:
                self._write()
    # helper to loop the heartbeat method
    def _heartbeat_loop(self):
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self.heartbeat()
            except Exception as exc:
                self.heartbeat_error = str(exc)

    # method to write the information at the end
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

    # class method enter
    def __enter__(self):
        return self.start()
    # class method exit
    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.finish(exit_code=0)
        elif isinstance(exc, KeyboardInterrupt):
            self.finish(exit_code=130, status='cancelled', error=exc)
        elif isinstance(exc, SystemExit):
            code = exc.code
            exit_code = int(code) if isinstance(code, int) else (0 if code is None else 1)
            self.finish(exit_code=exit_code, error=exc if exit_code else None)
        else:
            self.finish(exit_code=1, error=exc)
        return False
# ----------------------------------------------------------------------------------------------------------------------


class RunMonitor(MonitoringExecutionManager):
    """Retain the runner API and its unrounded, optional run timestamp."""

    def __init__(self, folder_name, *, run_name, application, run_time=None,
                 run_domain=None, run_mode=None, ensemble=None, task_id=None,
                 heartbeat_seconds=30, outputs=None):
        super().__init__(folder_name, None, run_name=run_name, application=application,
                         run_time=run_time, run_domain=run_domain, run_mode=run_mode,
                         ensemble=ensemble, task_id=task_id, heartbeat_seconds=heartbeat_seconds,
                         outputs=outputs, time_rounding=None)
        if run_time is None:
            self.run_time = self.record['run_time'] = None

    def __exit__(self, exc_type, exc, traceback):
        self.finish(0 if exc is None else None,
                    status='cancelled' if isinstance(exc, (KeyboardInterrupt, SystemExit)) else None,
                    error=exc)
        return False
