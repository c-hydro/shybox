"""Tests for the Bash observation downloader launcher."""

import fcntl
import json
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


LAUNCHER = Path(__file__).resolve().parents[2] / 'workflow/downloader/exec_downloader_workflow_vda_obs_ws.sh'
CVA_LAUNCHER = LAUNCHER.with_name('exec_downloader_workflow_cva_obs_dam.sh')


@unittest.skipUnless(shutil.which('flock'), 'flock is required')
class TestDownloaderLauncher(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.locks = self.root / 'locks'
        self.reports = self.root / 'reports'
        self.invocation = self.root / 'invocation.json'
        python = self.root / 'python'
        python.write_text(
            '#!/usr/bin/env python3\n'
            'import json, os, sys\n'
            'from pathlib import Path\n'
            'with Path(os.environ["LAUNCHER_TEST_INVOCATION"]).open("x") as output:\n'
            '    json.dump({\n'
            '        "args": sys.argv[1:], "time_run": os.environ["TIME_RUN"],\n'
            '        "time_period": os.environ["TIME_PERIOD"],\n'
            '        "domain": os.environ["DOMAIN_NAME"], "data": os.environ["PATH_DATA"],\n'
            '        "log": os.environ["PATH_LOG"],\n'
            '    }, output)\n'
            'sys.exit(int(os.environ.get("LAUNCHER_TEST_EXIT_CODE", "0")))\n',
            encoding='utf-8',
        )
        python.chmod(0o700)
        self.env = {
            **os.environ,
            'SHYBOX_PYTHON': str(python),
            'SHYBOX_CONDA_ENV': 'shybox_base_libraries',
            'SHYBOX_LOCK_DIR': str(self.locks),
            'SHYBOX_REPORT_DIR': str(self.reports),
            'PATH_LOG': str(self.root / 'logs'),
            'PATH_DATA': str(self.root / 'data'),
            'LAUNCHER_TEST_INVOCATION': str(self.invocation),
        }
        self.env.pop('TIME_PERIOD', None)
        self.env.pop('SHYBOX_RUN_VARIABLE', None)
        self.env.pop('SHYBOX_OBS_SETTINGS_FILE', None)
        self.env.pop('LAUNCHER_TEST_EXIT_CODE', None)

    def launch(self, *args, launcher=LAUNCHER):
        return subprocess.run(
            [str(launcher), *args], cwd=self.root, env=self.env,
            capture_output=True, text=True, timeout=15,
        )

    def read_report(self, name):
        keys = ('STATUS', 'RUN_NAME', 'RUN_VARIABLE', 'TIME_RUN', 'TIME_START', 'TIME_END', 'TIME_PERIOD',
                'EXECUTION_TIME', 'EXIT_CODE')
        result = subprocess.run(
            ['bash', '-ec', 'source "$1"; shift; for name; do printf "%s\\n" "${!name}"; done',
             'bash', str(self.reports / name), *keys],
            capture_output=True, text=True, check=True,
        )
        return dict(zip(keys, result.stdout.splitlines(), strict=True))

    def test_cva_realtime_uses_the_dam_server_profile(self):
        self.env.pop('PATH_DATA')
        zone = ZoneInfo('Europe/Rome')
        before = datetime.now(zone).strftime('%Y-%m-%d %H:00')
        result = self.launch(launcher=CVA_LAUNCHER)
        after = datetime.now(zone).strftime('%Y-%m-%d %H:00')

        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads(self.invocation.read_text())
        folder = '/home/idrologia/share/FloodProofs_v4'
        self.assertEqual(invocation['args'], [
            '-B', f'{folder}/library/package_shybox/workflow/downloader/app_downloader_workflow_cva_obs_dam_main.py',
            '-settings_file', f'{folder}/fp_tools_downloading/ground_network/app_downloader_workflow_cva_obs_dam_server.json',
            '-time', invocation['time_run'],
        ])
        self.assertEqual(invocation['data'], f'{folder}/data/data_dynamic/source/ground_network/dams/')
        self.assertIn(invocation['time_run'], {before, after})
        self.assertEqual(invocation['time_period'], '4')
        hour = datetime.fromisoformat(invocation['time_run']).strftime('%Y%m%d_%H')
        self.assertTrue((self.locks / f'lock_realtime_dam_{hour}.lock').is_file())
        report = self.read_report(f'report_realtime_dam_{hour}.env')
        self.assertEqual((report['STATUS'], report['RUN_VARIABLE']), ('success', 'dam'))

    def test_cva_history_records_failure_and_accepts_a_run_label(self):
        self.env.update(LAUNCHER_TEST_EXIT_CODE='17', SHYBOX_RUN_VARIABLE='turbine_discharge')
        settings = CVA_LAUNCHER.with_name('app_downloader_workflow_cva_obs_dam_local.json')
        start, end = '2026-11-25 11:00', '2026-11-25 14:00'
        result = self.launch('--settings-file', str(settings), 'history', start, end, launcher=CVA_LAUNCHER)

        self.assertEqual(result.returncode, 17, result.stderr)
        invocation = json.loads(self.invocation.read_text())
        self.assertEqual(invocation['args'][-4:], ['-settings_file', str(settings), '-time', end])
        self.assertEqual((invocation['time_run'], invocation['time_period']), (end, '3'))
        self.assertTrue((self.locks / 'lock_history_turbine_discharge_20261125_14.lock').is_file())
        report = self.read_report('report_history_turbine_discharge_20261125_14.env')
        self.assertEqual((report['TIME_START'], report['TIME_END']), (start, end))
        self.assertEqual((report['STATUS'], report['EXIT_CODE']), ('failed', '17'))

    def test_cva_history_preserves_the_report_while_locked(self):
        self.locks.mkdir()
        self.reports.mkdir()
        report = self.reports / 'report_history_dam_20261125_14.env'
        report.write_text('STATUS=running\n', encoding='utf-8')
        with (self.locks / 'lock_history_dam_20261125_14.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.launch('history', '2026-11-25 11:00', '2026-11-25 14:00', launcher=CVA_LAUNCHER)

        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertEqual(report.read_text(encoding='utf-8'), 'STATUS=running\n')
        self.assertFalse(self.invocation.exists())

    def test_realtime_floors_current_hour_and_defaults_to_four(self):
        zone = ZoneInfo('Europe/Rome')
        before = datetime.now(zone).strftime('%Y-%m-%d %H:00')
        result = self.launch()
        after = datetime.now(zone).strftime('%Y-%m-%d %H:00')
        self.assertEqual(result.returncode, 0, result.stderr)
        command_line = next(line for line in result.stdout.splitlines() if line.startswith(' ==> COMMAND LINE:'))
        invocation = json.loads(self.invocation.read_text())
        self.assertEqual(shlex.split(command_line.split(':', 1)[1]), [self.env['SHYBOX_PYTHON'], *invocation['args']])
        self.assertIn(' ==> START ...', result.stdout)
        self.assertIn(' ==> ... END', result.stdout)
        self.assertIn(' ==> Bye, Bye', result.stdout)
        report_file, = self.reports.glob('report_realtime_*.env')
        report = self.read_report(report_file.name)
        self.assertIn(report['TIME_RUN'], {before, after})
        self.assertEqual(report['TIME_PERIOD'], '4')
        self.assertEqual(report['RUN_VARIABLE'], 'rain')
        self.assertEqual((report['RUN_NAME'], report['STATUS'], report['EXIT_CODE']), ('realtime', 'success', '0'))
        self.assertEqual(
            json.loads(self.invocation.read_text())['args'],
            [
                '-B',
                '/home/idrologia/share/FloodProofs_v4/library/package_shybox/workflow/downloader/'
                'app_downloader_workflow_vda_obs_ws_main.py',
                '-settings_file',
                '/home/idrologia/share/FloodProofs_v4/fp_tools_downloading/ground_network/'
                'app_downloader_workflow_vda_obs_ws_server.json',
                '-time', report['TIME_RUN'],
            ],
        )
        run_time = datetime.fromisoformat(report['TIME_RUN'])
        self.assertEqual(run_time - datetime.fromisoformat(report['TIME_START']), timedelta(hours=4))
        hour = run_time.strftime('%Y%m%d_%H')
        self.assertTrue((self.locks / f'lock_realtime_rain_{hour}.lock').is_file())
        self.assertEqual(report_file.name, f'report_realtime_rain_{hour}.env')

    def test_history_passes_end_and_duration_and_records_app_failure(self):
        self.env['LAUNCHER_TEST_EXIT_CODE'] = '17'
        start, end = '2026-11-25 11:00', '2026-11-25 14:00'
        settings = LAUNCHER.with_name('app_downloader_workflow_vda_obs_ws_local.json')
        result = self.launch('history', start, end, '--settings-file', str(settings))
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertNotIn(' ==> ... END', result.stdout)
        invocation = json.loads(self.invocation.read_text())
        self.assertEqual(invocation['args'][-4:], ['-settings_file', str(settings), '-time', end])
        self.assertEqual((invocation['time_run'], invocation['time_period']), (end, '3'))
        report = self.read_report('report_history_rain_20261125_14.env')
        self.assertEqual((report['TIME_START'], report['TIME_END']), (start, end))
        self.assertEqual((report['STATUS'], report['EXIT_CODE']), ('failed', '17'))
        self.assertGreaterEqual(int(report['EXECUTION_TIME']), 0)

    def test_history_rejects_non_hourly_dates(self):
        result = self.launch('history', '2026-11-25 11:30', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 2)
        self.assertIn('hourly dates', result.stderr)
        self.assertFalse(self.invocation.exists())

    def test_launcher_exports_the_server_defaults(self):
        for name in ('DOMAIN_NAME', 'PATH_DATA', 'PATH_LOG'):
            self.env.pop(name, None)
        result = self.launch()
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads(self.invocation.read_text())
        folder = '/home/idrologia/share/FloodProofs_v4'
        self.assertEqual(invocation['domain'], 'vda')
        self.assertEqual(invocation['data'], f'{folder}/data/data_dynamic/source/ground_network/weather_stations/')
        self.assertEqual(invocation['log'], f'{folder}/logs/downloader/ground_network/')

    def test_lock_contention_preserves_the_existing_record(self):
        self.locks.mkdir()
        self.reports.mkdir()
        report = self.reports / 'report_history_rain_20261125_14.env'
        report.write_text('STATUS=running\n', encoding='utf-8')
        with (self.locks / 'lock_history_rain_20261125_14.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.launch('history', '2026-11-25 11:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertEqual(report.read_text(encoding='utf-8'), 'STATUS=running\n')
        self.assertFalse(self.invocation.exists())

    def test_run_variable_changes_the_lock_and_report_names(self):
        self.env['SHYBOX_RUN_VARIABLE'] = 'snow'
        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00')

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.locks / 'lock_history_snow_20261125_14.lock').is_file())
        report = self.read_report('report_history_snow_20261125_14.env')
        self.assertEqual(report['RUN_VARIABLE'], 'snow')

    def test_unavailable_conda_is_recorded_before_running_the_app(self):
        self.env.pop('SHYBOX_PYTHON')
        self.env['SHYBOX_CONDA_BIN'] = str(self.root / 'missing-conda')
        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 127)
        report = self.read_report('report_history_rain_20261125_14.env')
        self.assertEqual((report['STATUS'], report['EXIT_CODE'], report['TIME_PERIOD']), ('failed', '127', '0'))
        self.assertFalse(self.invocation.exists())

    def test_default_launch_uses_the_configured_interpreter_directly(self):
        launcher = self.root / LAUNCHER.name
        script = LAUNCHER.read_text()
        server_python = (
            '/home/idrologia/share/FloodProofs_v4/library/conda_shybox_base/'
            'envs/shybox_base_libraries/bin/python'
        )
        self.assertEqual(script.count(server_python), 1)
        python = self.env.pop('SHYBOX_PYTHON')
        self.env.pop('SHYBOX_CONDA_BIN', None)
        launcher.write_text(script.replace(server_python, python))
        launcher.chmod(0o700)

        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00', launcher=launcher)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.invocation.is_file())

    def test_unavailable_named_environment_is_recorded_before_running_the_app(self):
        conda = self.root / 'conda'
        conda.write_text('#!/bin/bash\n[[ "$*" == "run --no-capture-output --name unavailable-env python -B "* ]]\nexit 1\n',
                         encoding='utf-8')
        conda.chmod(0o700)
        self.env.pop('SHYBOX_PYTHON')
        self.env['SHYBOX_CONDA_BIN'] = str(conda)
        self.env['SHYBOX_CONDA_ENV'] = 'unavailable-env'
        result = self.launch('history', '2026-11-25 14:00', '2026-11-25 14:00')
        self.assertEqual(result.returncode, 1)
        report = self.read_report('report_history_rain_20261125_14.env')
        self.assertEqual((report['STATUS'], report['EXIT_CODE']), ('failed', '1'))
        self.assertFalse(self.invocation.exists())
