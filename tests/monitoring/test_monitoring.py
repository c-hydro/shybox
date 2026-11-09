"""Lifecycle records, HAT compatibility and monitoring summaries."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from datetime import timedelta
from unittest.mock import patch

from shybox.monitoring_toolkit.execution import RunMonitor, MonitoringCatalog, inspect_legacy_run, read_status
from shybox.monitoring_toolkit.common.io import utc_now, write_status


class MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.spec = {'run_name': 'icon_marche', 'application': 'hmc'}

    def monitor(self, **kwargs):
        return RunMonitor(self.root, **self.spec, run_time='2026-11-06T12:00:00Z', **kwargs)

    def catalog(self, **kwargs):
        return MonitoringCatalog({'folder': str(self.root), 'applications': {'hmc': {**self.spec, **kwargs}}})

    def test_success_and_missing_outputs_are_separate(self):
        monitor = self.monitor(outputs=[self.root / 'not-yet.nc'])
        with monitor:
            record = read_status(monitor.file_path)
            self.assertEqual(record['status'], 'running')
            self.assertIsNone(record['exit_code'])
        record = read_status(monitor.file_path)
        self.assertEqual(record['status'], 'succeeded')
        self.assertEqual(record['exit_code'], 0)
        self.assertEqual(record['output_status'], 'missing')
        self.assertIsNotNone(record['finished_at'])
        self.assertGreaterEqual(record['duration_seconds'], 0)

    def test_exception_records_failure_and_propagates(self):
        monitor = self.monitor()
        with self.assertRaisesRegex(RuntimeError, 'bad input'):
            with monitor:
                raise RuntimeError('bad input')
        record = read_status(monitor.file_path)
        self.assertEqual(record['status'], 'failed')
        self.assertIsNone(record['exit_code'])
        self.assertIn('bad input', record['error'])
        self.assertFalse(monitor._thread.is_alive())

    def test_heartbeat_and_stale_are_not_failure(self):
        monitor = self.monitor(heartbeat_seconds=.02).start()
        self.addCleanup(monitor.finish, None)
        first = read_status(monitor.file_path)['updated_at']
        deadline = time.monotonic() + 2
        while read_status(monitor.file_path)['updated_at'] == first and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertNotEqual(read_status(monitor.file_path)['updated_at'], first)
        result = self.catalog().summary(now=utc_now() + timedelta(seconds=200))
        self.assertEqual(result['applications'][0]['status'], 'stale')
        self.assertIsNone(result['executions'][0]['exit_code'])
        self.assertEqual(read_status(monitor.file_path)['status'], 'running')

    def test_unique_attempts_members_and_retries(self):
        one = self.monitor(ensemble=1).start(); one.finish(1)
        two = self.monitor(ensemble=1).start(); two.finish(0)
        three = self.monitor(ensemble=2).start(); three.finish(0)
        self.assertEqual(len({one.file_path, two.file_path, three.file_path}), 3)
        result = self.catalog(ensemble_members=[1, 2, 3]).summary()
        self.assertEqual(result['applications'][0]['counts'], {'succeeded': 2, 'missing': 1})
        self.assertEqual(result['applications'][0]['status'], 'missing')
        self.assertEqual(len(result['executions']), 3)

    def test_missing_expected_initialization_is_not_previous_success(self):
        with self.monitor():
            pass
        result = self.catalog(expected_run_time='2026-11-07T12:00:00Z').summary()
        self.assertEqual(result['applications'][0]['status'], 'missing')
        self.assertIsNone(result['applications'][0]['latest'])
        self.assertEqual(self.catalog().summary(date='2026-11-07')['executions'], [])

    def test_older_run_retry_does_not_replace_latest_initialization(self):
        with self.monitor():
            pass
        with RunMonitor(self.root, **self.spec, run_time='2026-11-05T12:00:00Z') as old:
            old.finish(2)
        self.assertEqual(self.catalog().summary()['applications'][0]['status'], 'succeeded')

    def test_atomic_write_failure_preserves_previous_record(self):
        path = self.root / 'record.json'
        write_status(path, {'previous': True})
        with self.assertRaises(ValueError):
            write_status(path, {'value': float('nan')})
        self.assertEqual(json.loads(path.read_text()), {'previous': True})
        self.assertEqual(list(self.root.iterdir()), [path])

    def test_invalid_records_do_not_hide_valid_runs(self):
        with self.monitor() as monitor:
            pass
        (self.root / 'bad.json').write_text('{')
        invalid = json.loads(monitor.file_path.read_text())
        invalid['updated_at'] = 123
        write_status(self.root / 'bad-time.json', invalid)
        result = self.catalog().summary()
        self.assertEqual(len(result['issues']), 2)
        self.assertEqual(result['applications'][0]['status'], 'succeeded')

    def test_legacy_reference_files_do_not_claim_exit_success(self):
        start, output = self.root / 'marche.info.txt', self.root / 'hydrograph.json'
        start.write_text('legacy start'); output.write_text('{}')
        os.utime(start, (1000, 1000)); os.utime(output, (1060, 1060))
        result = inspect_legacy_run(start, [output])
        self.assertEqual(result['estimated_duration_seconds'], 60)
        self.assertEqual(result['output_status'], 'available')
        self.assertEqual(result['status'], 'unknown')
        self.assertIsNone(result['exit_code'])

    def test_lightweight_imports_do_not_require_gis_adapters(self):
        import subprocess
        import sys
        script = """
import builtins
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split('.')[0] in ('osgeo', 'rioxarray'):
        raise ImportError('GIS deliberately unavailable')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from shybox.monitoring_toolkit.execution import RunMonitor
import importlib.util
if importlib.util.find_spec('flask'):
    from hydrapp.app import create_app
"""
        subprocess.run([sys.executable, '-B', '-c', script], check=True, capture_output=True)

    def test_legacy_configuration_is_reported_only_without_modern_records(self):
        spec = {**self.spec, 'legacy': {'start_file': str(self.root / 'start'), 'output_files': []}}
        catalog = MonitoringCatalog({'applications': {'legacy': spec}})
        result = catalog.summary()
        self.assertEqual(result['applications'][0]['status'], 'unknown')
        self.assertEqual(result['applications'][0]['legacy']['output_status'], 'unknown')
        result = catalog.summary(date='2026-11-06')
        self.assertEqual(result['applications'][0]['status'], 'missing')

    def test_task_context_and_unknown_exit_code(self):
        monitor = self.monitor(task_id='dask-task-a', ensemble=4)
        monitor.start(); monitor.finish()
        record = read_status(monitor.file_path)
        self.assertEqual(record['task_id'], 'dask-task-a')
        self.assertEqual(record['status'], 'failed')
        with self.assertRaises(ValueError):
            self.monitor().finish(status='succeeded')


if __name__ == '__main__':
    unittest.main()
