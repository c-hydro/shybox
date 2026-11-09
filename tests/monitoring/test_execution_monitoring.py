"""Actual subprocess execution through the runner's monitoring integration."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.monitoring_toolkit.execution import read_status
from shybox.runner_toolkit.execution.execution_handler import ExecutionManager


class ExecutionMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.logger = LoggingManager(name='monitoring_test', level=50)
        self.enterContext(self.logger.use_as_current())

    def manager(self, script='echo done\nexit 0', **kwargs):
        source = self.root / 'library' / 'program.sh'
        source.parent.mkdir(exist_ok=True)
        source.write_text('#!/bin/sh\n' + script + '\n')
        source.chmod(0o755)
        obj = {'description': {'execution_name': 'hmc', 'execution_mode': 'deterministic'},
               'executable': {'location': str(self.root / 'exec' / 'program.sh'), 'arguments': ''},
               'library': {'location': str(source), 'dependencies': []},
               'info': {'location': str(self.root / 'exec' / 'execution.info')},
               'monitoring': {'enabled': True, 'folder_name': str(self.root / 'monitoring'),
                              'run_name': 'icon_marche', 'application': 'hmc'}}
        return ExecutionManager(obj, time_obj='2026-11-06T12:00:00Z', stream_output=False,
                                logger=self.logger, **kwargs)

    def record(self, manager):
        return read_status(manager.monitor.file_path)

    def test_buffered_success_records_real_exit_code(self):
        manager = self.manager()
        result = manager.run()
        self.assertEqual(result['exec_response'][2], '0')
        self.assertEqual(self.record(manager)['status'], 'succeeded')
        self.assertEqual(self.record(manager)['exit_code'], 0)

    def test_streaming_nonzero_exit_without_stderr_is_failure(self):
        manager = self.manager('echo failed\nexit 7')
        manager.stream_output = True
        result = manager.run()
        self.assertEqual(result['exec_response'][2], '7')
        self.assertEqual(self.record(manager)['exit_code'], 7)
        self.assertEqual(self.record(manager)['status'], 'failed')

    def test_stderr_failure_preserves_code_and_exception(self):
        manager = self.manager('echo bad >&2\nexit 2')
        with self.assertRaises(RuntimeError):
            manager.run()
        self.assertEqual(self.record(manager)['status'], 'failed')
        self.assertEqual(self.record(manager)['exit_code'], 2)
        self.assertIn('bad', self.record(manager)['error'])

    def test_skip_and_dry_run_do_not_claim_process_execution(self):
        manager = self.manager()
        manager.run()
        manager.execution_update = False
        manager.run()
        record = self.record(manager)
        self.assertEqual(record['status'], 'skipped')
        self.assertIsNone(record['started_at'])
        self.assertIsNone(record['exit_code'])
        manager.run(dry_run=True)
        record = self.record(manager)
        self.assertEqual(record['status'], 'dry_run')
        self.assertIsNone(record['started_at'])
        self.assertIsNone(record['exit_code'])

    def test_preflight_failure_is_recorded(self):
        manager = self.manager()
        with patch.object(manager, '_prepare_executable', side_effect=FileNotFoundError('no program')):
            with self.assertRaises(FileNotFoundError):
                manager.run()
        self.assertEqual(self.record(manager)['status'], 'failed')
        self.assertIsNone(self.record(manager)['exit_code'])

    def test_timeout_is_failure_and_propagates(self):
        import subprocess
        manager = self.manager('sleep .2', timeout=.02)
        with self.assertRaises(subprocess.TimeoutExpired):
            manager.run()
        self.assertEqual(self.record(manager)['status'], 'failed')
        self.assertIsNone(self.record(manager)['exit_code'])

    def test_quiet_streaming_timeout_stops_process_and_records_failure(self):
        import subprocess
        manager = self.manager('exec sleep 2', timeout=.03)
        manager.stream_output = True
        with self.assertRaises(subprocess.TimeoutExpired):
            manager.run()
        record = self.record(manager)
        self.assertEqual(record['status'], 'failed')
        self.assertLess(record['exit_code'], 0)
        self.assertFalse(manager.monitor._thread.is_alive())

    def test_monitoring_io_failure_does_not_replace_execution_result(self):
        manager = self.manager()
        with patch('shybox.monitoring_toolkit.execution.handler.write_status', side_effect=OSError('read only')):
            result = manager.run()
        self.assertEqual(result['exec_response'][2], '0')
        self.assertIsNone(manager.monitor)

    def test_disabled_monitoring_keeps_existing_runner_behaviour(self):
        manager = self.manager(monitoring={'enabled': False})
        manager.run()
        self.assertIsNone(manager.monitor)
        self.assertFalse((self.root / 'monitoring').exists())


if __name__ == '__main__':
    unittest.main()
