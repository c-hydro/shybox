"""Run the lifecycle monitoring example with JSON settings and a CLI time override."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from shybox.monitoring_toolkit.lib_monitoring_io import read_status


class TestMonitoringMainExample(unittest.TestCase):
    def setUp(self):
        self.repo = Path(__file__).resolve().parents[2]
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        example = self.repo / 'workflow' / 'monitoring' / 'app_monitoring.example_run.json'
        settings = json.loads(example.read_text())
        settings['configuration']['monitoring'].update({
            'path': str(self.folder / 'records'), 'total_seconds': 0, 'run_name': 'test_run',
            'run_time': '2026-11-20T12:00:00Z', 'ensemble': 4, 'run_mode': 'probabilistic',
        })
        self.config = self.folder / 'settings.json'
        self.config.write_text(json.dumps(settings))

    def run_example(self, *args):
        return subprocess.run(
            [sys.executable, '-B', '-m', 'workflow.monitoring.app_monitoring_main_example',
             '-settings_file', str(self.config), *args],
            cwd=self.repo, env={**os.environ, 'TMPDIR': str(self.folder)},
            capture_output=True, text=True, timeout=10,
        )

    def test_json_settings_drive_record_and_output_folder(self):
        result = self.run_example()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        path, = (self.folder / 'records').glob('*.json')
        record = read_status(path)
        self.assertEqual(record['run_name'], 'test_run')
        self.assertEqual(record['run_time'], '2026-11-20T12:00:00.000000Z')
        self.assertEqual(record['run_mode'], 'probabilistic')
        self.assertEqual(record['ensemble'], 4)
        self.assertEqual(record['status'], 'succeeded')
        self.assertFalse((self.folder / 'shybox' / 'logs' / 'app_monitoring_main_example.log').exists())

    def test_partial_log_section_uses_temp_path_and_preserves_filename(self):
        settings = json.loads(self.config.read_text())
        settings['configuration']['log'] = {'path': None, 'file_name': 'demo.log', 'handlers': ['file']}
        self.config.write_text(json.dumps(settings))
        result = self.run_example()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        log = self.folder / 'shybox' / 'logs' / 'demo.log'
        self.assertIn('Application completed successfully.', log.read_text())

    def test_cli_arguments_override_json_and_failure_is_recorded(self):
        folder = self.folder / 'override'
        settings = json.loads(self.config.read_text())
        settings['configuration']['monitoring'].update(path=str(folder), ensemble=7, fail=True)
        self.config.write_text(json.dumps(settings))
        result = self.run_example('-time', '2026-11-20T13:00:00Z')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        path, = folder.glob('*.json')
        record = read_status(path)
        self.assertEqual(record['run_time'], '2026-11-20T13:00:00.000000Z')
        self.assertEqual(record['ensemble'], 7)
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['exit_code'], 1)
