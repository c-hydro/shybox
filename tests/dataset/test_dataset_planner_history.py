"""Persist and restore complete availability plans without rechecking files."""
from pathlib import Path
import pickle
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import test_dataset_planner as fixtures


class TestDatasetPlannerHistory(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.history = self.root / 'plans_file.workspace'
        self.fixture = fixtures.TestDatasetPlannerAvailability()
        self.fixture.setUp()

    def update(self, planner, **kwargs):
        return planner.update_time(3, 1, pickle_history=self.history, **kwargs)

    def test_saved_plan_restores_adjustments_and_file_availability_without_probes(self):
        original = self.fixture.planner()
        self.update(original, exists=self.fixture.probe)
        self.assertTrue(self.history.is_file())
        restored = self.fixture.planner()
        never = Mock(side_effect=AssertionError('unexpected file check'))
        self.update(restored, reset_history=False, exists=never)
        never.assert_not_called()
        pd.testing.assert_frame_equal(original.time_table, restored.time_table)
        pd.testing.assert_frame_equal(original.file_plan, restored.file_plan)
        pd.testing.assert_index_equal(original.time_plan.observed, restored.time_plan.observed)
        pd.testing.assert_index_equal(original.time_plan.forecast, restored.time_plan.forecast)
        self.assertEqual(original.check_count, restored.check_count)
        self.assertTrue(any('history loaded' in str(call) for call in self.fixture.logger.info.call_args_list))

    def test_default_reset_rechecks_files_and_replaces_history(self):
        planner = self.fixture.planner()
        self.update(planner, exists=self.fixture.probe)
        before = self.history.read_bytes()
        self.assertTrue(planner.time_table.time_adjusted.any())
        probe = Mock(return_value=True)
        self.update(planner, exists=probe)
        self.assertTrue(probe.called)
        self.assertFalse(planner.time_table.time_adjusted.any())
        self.assertNotEqual(before, self.history.read_bytes())

    def test_disabled_history_neither_reads_nor_writes_workspace(self):
        self.history.write_bytes(b'untouched')
        probe = Mock(return_value=True)
        self.update(self.fixture.planner(), flag_history=False, reset_history=False, exists=probe)
        self.assertTrue(probe.called)
        self.assertEqual(self.history.read_bytes(), b'untouched')
        self.history.unlink()
        self.update(self.fixture.planner(), flag_history=False, exists=probe)
        self.assertFalse(self.history.exists())

    def test_missing_or_corrupt_history_is_recomputed(self):
        for content in (None, b'not a pickle', pickle.dumps({'version': 1})):
            with self.subTest(content=content):
                if content is not None:
                    self.history.write_bytes(content)
                probe = Mock(return_value=True)
                self.update(self.fixture.planner(), reset_history=False, exists=probe)
                self.assertTrue(probe.called)
                with self.history.open('rb') as stream:
                    saved = pickle.load(stream)
                self.assertIn('time_plan', saved)
                self.assertIn('file_plan', saved)

    def test_changed_window_does_not_reuse_history(self):
        self.update(self.fixture.planner(), exists=self.fixture.probe)
        planner = self.fixture.planner()
        probe = Mock(return_value=True)
        planner.update_time(0, 0, reset_history=False, pickle_history=self.history, exists=probe)
        self.assertTrue(probe.called)
        self.assertEqual(int(planner.time_table.time_checked.sum()), 1)

    def test_changed_source_configuration_does_not_reuse_history(self):
        self.update(self.fixture.planner(), exists=self.fixture.probe)
        self.fixture.config['data_src_obs']['rain']['file_name'] = 'new.{time:%H}.nc'
        planner = self.fixture.planner()
        probe = Mock(return_value=True)
        self.update(planner, reset_history=False, exists=probe)
        self.assertTrue(probe.called)
        observed = planner.file_plan.query('group == "observed"')
        self.assertTrue(observed.file_source.str.contains('/new.', regex=False).all())

    def test_failed_atomic_save_preserves_existing_workspace(self):
        self.update(self.fixture.planner(), exists=self.fixture.probe)
        before = self.history.read_bytes()
        with patch('os.replace', side_effect=OSError('cannot publish workspace')):
            result = self.update(self.fixture.planner(), exists=lambda path: True)
        self.assertIsNotNone(result)
        self.assertEqual(self.history.read_bytes(), before)
        self.assertEqual(list(self.root.iterdir()), [self.history])

    def test_history_flags_require_booleans(self):
        for name in ('reset_history', 'flag_history'):
            with self.subTest(name=name), self.assertRaisesRegex(TypeError, name):
                self.update(self.fixture.planner(), **{name: 1})


if __name__ == '__main__':
    unittest.main()
