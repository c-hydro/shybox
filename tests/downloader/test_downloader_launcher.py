"""Tests for the Bash observation downloader launcher."""

import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


LAUNCHER = Path(__file__).resolve().parents[2] / 'workflow/downloader/exec_downloader_workflow_obs.sh'


@unittest.skipUnless(shutil.which('flock'), 'flock is required')
class TestDownloaderLauncher(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.runs = self.root / 'runs'
        self.locks = self.root / 'locks'
        self.reports = self.root / 'reports'
        self.invocation = self.root / 'invocation.json'
        self.launcher = self.root / LAUNCHER.name
        launcher_source = LAUNCHER.read_text(encoding='utf-8')
        checkout_paths = {
            'script_folder': LAUNCHER.parents[2],
            'script_file': LAUNCHER.with_name('app_downloader_workflow_obs_main.py'),
            'settings_file': LAUNCHER.with_name('app_downloader_workflow_obs_rain_server.json'),
        }
        for setting, target in checkout_paths.items():
            launcher_source, count = re.subn(
                rf'^{setting}=.*$', lambda _: f'{setting}={shlex.quote(str(target))}',
                launcher_source, count=1, flags=re.MULTILINE,
            )
            self.assertEqual(count, 1, f'Missing launcher setting {setting}')
        self.launcher.write_text(launcher_source, encoding='utf-8')
        self.launcher.chmod(0o700)
        python = self.root / 'python'
        python.write_text(
            '#!/usr/bin/env python3\n'
            'import json, os, sys\n'
            'from pathlib import Path\n'
            'if sys.argv[1:3] == ["-B", "-c"]:\n'
            '    sys.exit(0)\n'
            'Path(os.environ["LAUNCHER_TEST_INVOCATION"]).write_text(json.dumps({\n'
            '    "args": sys.argv[1:], "time_run": os.environ["TIME_RUN"],\n'
            '    "time_period": os.environ["TIME_PERIOD"],\n'
            '}))\n'
            'sys.exit(int(os.environ.get("LAUNCHER_TEST_EXIT_CODE", "0")))\n',
            encoding='utf-8',
        )
        python.chmod(0o700)
        self.env = {
            **os.environ,
            'SHYBOX_PYTHON': str(python),
            'SHYBOX_CONDA_ENV': 'shybox_base_libraries',
            'SHYBOX_RUN_DIR': str(self.runs),
            'SHYBOX_LOCK_DIR': str(self.locks),
            'SHYBOX_REPORT_DIR': str(self.reports),
            'PATH_LOG': str(self.root / 'logs'),
            'PATH_DATA': str(self.root / 'data'),
            'LAUNCHER_TEST_INVOCATION': str(self.invocation),
        }
        self.env.pop('TIME_PERIOD', None)
        self.env.pop('SHYBOX_OBS_SETTINGS_FILE', None)
        self.env.pop('LAUNCHER_TEST_EXIT_CODE', None)

    def launch(self, *args):
        return subprocess.run(
            [str(self.launcher), *args], cwd=self.root, env=self.env,
            capture_output=True, text=True, timeout=15,
        )

    def read_record(self, name):
        return json.loads((self.runs / name).read_text(encoding='utf-8'))

    def read_report(self, name):
        result = subprocess.run(
            ['bash', '-ec', 'source "$1"; printf "%s\\n" "$RUN_NAME" "$TIME_RUN" "$STATUS" "$EXIT_CODE" "$EXECUTION_TIME"',
             'bash', str(self.reports / name)],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.splitlines()

    def test_realtime_floors_current_hour_and_defaults_to_four(self):
        zone = ZoneInfo('Europe/Rome')
        before = datetime.now(zone).strftime('%Y-%m-%d %H:00')
        result = self.launch()
        after = datetime.now(zone).strftime('%Y-%m-%d %H:00')
        self.assertEqual(result.returncode, 0, result.stderr)
        run_file, = self.runs.glob('realtime_*.json')
        record = self.read_record(run_file.name)
        self.assertIn(record['time_run'], {before, after})
        self.assertEqual(record['time_period'], 4)
        self.assertEqual(record['status'], 'success')
        self.assertEqual(record['exit_code'], 0)
        self.assertEqual(record['settings_file'], str(LAUNCHER.with_name('app_downloader_workflow_obs_rain_server.json')))
        self.assertEqual(
            json.loads(self.invocation.read_text())['args'][1],
            str(LAUNCHER.with_name('app_downloader_workflow_obs_main.py')),
        )
        self.assertIsNotNone(record['finished_at'])
        start = datetime.fromisoformat(record['time_start'])
        self.assertEqual(datetime.fromisoformat(record['time_run']) - start, timedelta(hours=4))
        hour = datetime.fromisoformat(record['time_run']).strftime('%Y%m%d_%H')
        self.assertTrue((self.locks / f'lock_realtime_{hour}.lock').is_file())
        self.assertEqual(
            self.read_report(f'report_realtime_{hour}.env'),
            ['realtime', record['time_run'], 'success', '0', str(record['execution_time'])],
        )

    def test_history_passes_end_and_duration_and_records_app_failure(self):
        self.env['LAUNCHER_TEST_EXIT_CODE'] = '17'
        start, end = '2026-11-25 11:00', '2026-11-25 14:00'
        settings = LAUNCHER.with_name('app_downloader_workflow_obs_rain_local.json')
        result = self.launch('history', start, end, '--settings-file', str(settings))
        self.assertEqual(result.returncode, 17, result.stderr)
        invocation = json.loads(self.invocation.read_text())
        self.assertEqual(invocation['args'][-4:], ['-settings_file', str(settings), '-time', end])
        self.assertEqual((invocation['time_run'], invocation['time_period']), (end, '3'))
        record = self.read_record('history_202611251400.json')
        self.assertEqual((record['time_start'], record['time_end']), (start, end))
        self.assertEqual((record['status'], record['exit_code']), ('failed', 17))
        self.assertEqual(record['execution_time_unit'], 'seconds')
        self.assertGreaterEqual(record['execution_time'], 0)
        self.assertEqual(
            self.read_report('report_history_20261125_14.env'),
            ['history', end, 'failed', '17', str(record['execution_time'])],
        )

    def test_history_rejects_non_hourly_dates(self):
        result = self.launch('history', '2026-11-25 11:30', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 2)
        self.assertIn('hourly dates', result.stderr)
        self.assertFalse(self.invocation.exists())

    def test_lock_contention_preserves_the_existing_record(self):
        self.runs.mkdir()
        self.locks.mkdir()
        self.reports.mkdir()
        run_file = self.runs / 'history_202611251400.json'
        run_file.write_text('{"status": "running"}', encoding='utf-8')
        report = self.reports / 'report_history_20261125_14.env'
        report.write_text('STATUS=running\n', encoding='utf-8')
        with (self.locks / 'lock_history_20261125_14.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.launch('history', '2026-11-25 11:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertEqual(self.read_record(run_file.name), {'status': 'running'})
        self.assertEqual(report.read_text(encoding='utf-8'), 'STATUS=running\n')
        self.assertFalse(self.invocation.exists())

    def test_unavailable_conda_is_recorded_before_running_the_app(self):
        self.env.pop('SHYBOX_PYTHON')
        self.env['SHYBOX_CONDA_BIN'] = str(self.root / 'missing-conda')
        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 1)
        record = self.read_record('history_202611251400.json')
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['time_period'], 0)
        self.assertIn('Conda executable', record['message'])
        self.assertFalse(self.invocation.exists())

    def test_unavailable_named_environment_is_recorded_before_running_the_app(self):
        conda = self.root / 'conda'
        conda.write_text('#!/bin/bash\nexit 1\n', encoding='utf-8')
        conda.chmod(0o700)
        self.env.pop('SHYBOX_PYTHON')
        self.env['SHYBOX_CONDA_BIN'] = str(conda)
        self.env['SHYBOX_CONDA_ENV'] = 'unavailable-env'
        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 1)
        record = self.read_record('history_202611251400.json')
        self.assertEqual((record['status'], record['conda_environment']), ('failed', 'unavailable-env'))
        self.assertIn('unavailable-env', record['message'])
        self.assertFalse(self.invocation.exists())
