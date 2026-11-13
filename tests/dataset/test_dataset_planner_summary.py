"""Independent member summaries, optional merging and portable JSON I/O."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd

import test_dataset_planner as fixtures
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner


class TestDatasetPlannerSummary(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        fixture = fixtures.TestDatasetPlanner()
        fixture.setUp()
        self.config = fixture.config
        self.logger = Mock()
        for group in ('obs', 'frc'):
            source = self.config['data_src_' + group]['rain']
            source.update(path=str(self.root / 'input'), file_name=f'{group}.{{time:%H}}.nc')
            destination = self.config['data_dst_' + group]['forcing']
            destination.update(path=str(self.root / 'output'),
                               file_name='{time_mode}/forcing.{time:%H}.nc')

    def planner(self):
        return DatasetPlanner.from_config(**self.config, logger=self.logger)

    def collect(self, planner, *args, **kwargs):
        # Simulate the application collecting records after each member finishes.
        records = {}
        for member in planner.members or ('deterministic',):
            records[member] = planner.summary(*args, member=member, **kwargs)
        return records

    def ensemble_planner(self):
        self.config['data_src_frc']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='rain.{ensemble_member}.{time:%H}.nc')
        for group in ('obs', 'frc'):
            source = deepcopy(self.config['data_src_' + group]['rain'])
            source.update(variable={'temperature': 'air_t'}, mode='deterministic', ensemble=None,
                          file_name='temperature.{time:%H}.nc')
            self.config['data_src_' + group]['air_t'] = source
            destination = self.config['data_dst_' + group]['forcing']
            destination['variable']['air_t'] = 'Air_Temperature'
            destination['file_name'] = '{time_mode}/run_{ensemble_member}/forcing.{time:%H}.nc'
        return self.planner()

    def create_outputs(self, planner, alternate=False):
        for expected in planner.file_plan.file_destination.dropna().unique():
            if alternate:
                expected = expected[:-3] if expected.endswith('.gz') else expected + '.gz'
            path = Path(expected)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    def test_fresh_scan_exports_a_deterministic_record_and_preserves_planner_state(self):
        planner = self.planner()
        planner.check_availability(exists=lambda path: False)
        before_files, before_time, count = planner.file_plan, planner.time_table, planner.check_count
        self.assertFalse(self.collect(planner, )['deterministic']['ready'])
        self.create_outputs(planner)
        output = self.root / 'manifest' / 'run.json'
        summaries = self.collect(planner, output)
        self.assertEqual(list(summaries), ['deterministic'])
        summary = summaries['deterministic']
        self.assertEqual(summary['member'], 'deterministic')
        self.assertNotIn('members', summary)
        self.assertTrue(summary['ready'])
        self.assertEqual(summary['schema_version'], 1)
        self.assertEqual(summary['time_run'], planner.time_plan.time_run.isoformat())
        self.assertEqual(summary['time_start'], planner.time_period[0].isoformat())
        self.assertEqual(summary['time_end'], planner.time_period[-1].isoformat())
        self.assertEqual(summary['time_steps'], 3)
        self.assertEqual(summary['time_step_seconds'], 3600.)
        self.assertEqual(summary['counts']['available_files'], 3)
        self.assertEqual(DatasetPlanner.read_summary(output), summary)
        pd.testing.assert_frame_equal(before_files, planner.file_plan)
        pd.testing.assert_frame_equal(before_time, planner.time_table)
        self.assertEqual(planner.check_count, count)

    def test_accepts_both_compression_variants_and_records_actual_path(self):
        for planned_gzip in (False, True):
            with self.subTest(planned_gzip=planned_gzip):
                for group in ('obs', 'frc'):
                    self.config['data_dst_' + group]['forcing']['file_name'] = (
                        f'case_{planned_gzip}/forcing.{{time:%H}}.nc' + ('.gz' if planned_gzip else ''))
                planner = self.planner()
                self.create_outputs(planner, alternate=True)
                summary = self.collect(planner, time_mode='forecast')['deterministic']
                self.assertTrue(summary['ready'])
                for step in summary['steps']:
                    variable = step['variables']['rain']
                    self.assertNotEqual(variable['file_expected'], variable['file_actual'])
                    self.assertEqual(variable['file_actual'].endswith('.gz'), not planned_gzip)
                self.assertFalse(self.collect(planner, allow_compressed=False)['deterministic']['ready'])

    def test_each_member_has_complete_metadata_and_its_own_readiness_and_counts(self):
        planner = self.ensemble_planner()
        self.create_outputs(planner)
        paths = planner.file_by_ensemble('destination', time_mode='forecast')
        Path(paths['002']['2026-11-09 11:00:00']['rain']).unlink()
        summaries = self.collect(planner, time_mode='forecast')
        self.assertEqual(list(summaries), ['001', '002'])
        for member, record in summaries.items():
            self.assertEqual(record['member'], member)
            self.assertNotIn('members', record)
            self.assertEqual(record['time_end'], '2026-11-09T11:00:00')
            self.assertEqual(record['time_steps'], 2)
            self.assertEqual(record['counts']['expected_files'], 2)
        self.assertTrue(summaries['001']['ready'])
        self.assertFalse(summaries['002']['ready'])
        self.assertEqual(summaries['002']['missing_times'], ['2026-11-09T11:00:00'])
        self.assertEqual(summaries['002']['steps'][-1]['missing_variables'], ['rain', 'air_t'])
        self.assertEqual(summaries['002']['counts']['missing_files'], 1)
        self.assertEqual(summaries['002']['counts']['ready_steps'], 1)
        summaries['001']['execution']['local'] = 'only first member'
        self.assertNotIn('local', summaries['002']['execution'])

    def test_separate_files_hold_one_record_and_merge_file_is_keyed_by_member(self):
        planner = self.ensemble_planner()
        summaries = self.collect(planner, time_mode='forecast', exists=lambda path: True)
        before = deepcopy(summaries)
        planner.write_summary(self.root / '{member}' / 'inputs.json', summaries)
        for member in summaries:
            self.assertEqual(DatasetPlanner.read_summary(self.root / member / 'inputs.json'), summaries[member])
        merged = self.root / 'merged.json'
        planner.write_summary(merged, summaries, flag_merge=True)
        saved = DatasetPlanner.read_summary(merged)
        self.assertEqual(saved, summaries)
        self.assertEqual(set(saved), {'001', '002'})
        self.assertEqual(saved['001']['member'], '001')
        self.assertEqual(saved['002']['member'], '002')
        self.assertEqual(summaries, before)
        direct = self.root / 'direct.json'
        regenerated = planner.summary_execution(summaries, file_name=direct)
        self.assertEqual(json.loads(direct.read_text()), regenerated)

    def test_default_suffixes_and_deterministic_filename_placeholder(self):
        planner = self.ensemble_planner()
        planner.write_summary(self.root / 'auto.json', self.collect(planner, exists=lambda path: True))
        self.assertTrue((self.root / 'auto.001.json').is_file())
        self.assertTrue((self.root / 'auto.002.json').is_file())
        self.assertFalse((self.root / 'auto.json').exists())
        self.config['data_src_frc']['rain'].update(mode='deterministic', ensemble=None,
                                                   file_name='rain.{time:%H}.nc')
        for group in ('obs', 'frc'):
            self.config['data_dst_' + group]['forcing']['file_name'] = '{time_mode}/forcing.{time:%H}.nc'
        planner = self.planner()
        self.collect(planner, self.root / 'single.{ensemble_member}.json', exists=lambda path: True)
        saved = DatasetPlanner.read_summary(self.root / 'single.deterministic.json')
        self.assertEqual(saved['member'], 'deterministic')

    def test_filter_checks_destinations_only_and_deduplicates_probes(self):
        planner = self.ensemble_planner()
        calls = []

        def exists(path):
            self.assertIn('/output/', str(path))
            self.assertIn('/forecast/', str(path))
            calls.append(str(path))
            return True

        summaries = self.collect(planner, time_mode='forecast', exists=exists)
        for record in summaries.values():
            self.assertTrue(record['ready'])
            self.assertEqual(record['time_steps'], 2)
            self.assertEqual(record['coverage']['observed']['time_steps'], 0)
        self.assertEqual(len(calls), len(set(calls)))
        self.assertEqual(len(calls), 4)

    def test_empty_selection_is_not_ready(self):
        self.config['time']['time_look_forecast'] = '0h'
        summary = self.collect(self.planner(), time_mode='forecast')['deterministic']
        self.assertFalse(summary['ready'])
        self.assertIsNone(summary['time_start'])
        self.assertIsNone(summary['time_end'])
        self.assertEqual(summary['time_steps'], 0)

    def test_summary_keeps_forecast_replacements_from_the_adjusted_plan(self):
        fixture = fixtures.TestDatasetPlannerAvailability()
        fixture.setUp()
        planner = fixture.planner()
        planner.update_time(1, None, exists=fixture.probe, flag_history=False)
        before = planner.time_table
        summary = self.collect(planner, time_mode='forecast', exists=lambda path: True)['deterministic']
        self.assertTrue(summary['ready'])
        self.assertEqual(summary['time_start'], '2026-11-09T08:00:00')
        self.assertTrue(summary['steps'][0]['time_adjusted'])
        self.assertTrue(summary['steps'][1]['time_adjusted'])
        pd.testing.assert_frame_equal(before, planner.time_table)

    def test_absent_plan_rows_are_reported_as_missing_steps(self):
        planner = self.planner()
        planner.plan_files()
        planner._file_plan = planner.file_plan.loc[planner.file_plan.time.ne(planner.time_period[-1])]
        summary = self.collect(planner, exists=lambda path: True)['deterministic']
        self.assertFalse(summary['ready'])
        self.assertEqual(summary['steps'][-1]['missing_variables'], ['rain'])
        self.assertIsNone(summary['steps'][-1]['variables']['rain']['file_expected'])

    def test_atomic_export_preserves_old_manifest_on_failure(self):
        output = self.root / 'run.json'
        output.write_text('previous manifest')
        with patch('os.replace', side_effect=OSError('publication failed')):
            with self.assertRaisesRegex(OSError, 'publication failed'):
                self.collect(self.planner(), output, exists=lambda path: True)
        self.assertEqual(output.read_text(), 'previous manifest')
        self.assertEqual(list(self.root.iterdir()), [output])

    def test_extra_args_keep_json_scalars_and_warn_without_aborting(self):
        output = self.root / 'extra.json'
        valid = {'scenario': 'control', 'members': 20, 'threshold': 1.5, 'enabled': True, 'ready': False, 'nested': {}}
        invalid = {7: 'bad key', 'path': self.root, 'object': object(),
                   'list': [1], 'null': None, 'nan': float('nan'), 'infinity': float('inf')}
        summaries = self.collect(self.planner(), output, exists=lambda path: True, extra_args={**valid, **invalid})
        record = summaries['deterministic']
        self.assertTrue(record['ready'])
        self.assertEqual(record['execution'], valid)
        self.assertEqual(DatasetPlanner.read_summary(output), record)
        self.assertEqual(self.logger.warning.call_count, len(invalid))
        self.assertIn('path', invalid)

    def test_non_dictionary_extra_args_are_skipped_with_a_warning(self):
        summary = self.collect(self.planner(), exists=lambda path: True, extra_args=['invalid'])['deterministic']
        self.assertEqual(summary['execution'], {})
        self.assertTrue(summary['ready'])
        self.logger.warning.assert_called_once()

    def test_write_summary_accepts_single_records_and_merges_extras_without_rechecking(self):
        planner = self.planner()
        summaries = self.collect(planner, exists=lambda path: True, extra_args={'scenario': 'old', 'members': 2})
        before = deepcopy(summaries)
        output = self.root / 'written.json'
        with patch.object(planner, 'summary', side_effect=AssertionError('unexpected rescan')):
            written = planner.write_summary(output, summaries['deterministic'],
                extra_args={'scenario': 'new', 'members': object(), 'bad': float('nan')})
        self.assertEqual(written['deterministic']['execution'], {'scenario': 'new', 'members': 2})
        self.assertEqual(summaries, before)
        with patch.object(Path, 'is_file', side_effect=AssertionError('unexpected file probe')):
            self.assertEqual(DatasetPlanner.read_summary(output), written['deterministic'])

    def test_writer_validates_all_records_before_publishing_any_member(self):
        planner = self.ensemble_planner()
        summaries = self.collect(planner, exists=lambda path: True)
        summaries['002']['non_json'] = object()
        with self.assertRaises(TypeError):
            planner.write_summary(self.root / 'inputs.{member}.json', summaries)
        self.assertFalse((self.root / 'inputs.001.json').exists())
        self.assertFalse((self.root / 'inputs.002.json').exists())

    def test_writer_filters_preexisting_extras_and_preserves_files_on_core_errors(self):
        planner = self.planner()
        record = self.collect(planner, exists=lambda path: True)['deterministic']
        output = self.root / 'written.json'
        record['execution'] = {'keep': 3, 'bad': object()}
        written = planner.write_summary(output, record)
        self.assertEqual(written['deterministic']['execution'], {'keep': 3})
        self.assertIn('bad', record['execution'])
        before = output.read_bytes()
        record['steps'] = object()
        with self.assertRaises(TypeError):
            planner.write_summary(output, record)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(list(self.root.iterdir()), [output])

    def test_invalid_flags_templates_collisions_and_schema_are_rejected(self):
        planner = self.ensemble_planner()
        summaries = self.collect(planner, exists=lambda path: True)
        for options in ({'time_mode': 'invalid'}, {'allow_compressed': 'yes'}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                self.collect(planner, **options)
        with self.assertRaisesRegex(ValueError, 'placeholders'):
            planner.write_summary(self.root / '{unknown}.json', summaries)
        with self.assertRaisesRegex(ValueError, 'flag_merge'):
            planner.write_summary(self.root / '{member}.json', summaries, flag_merge=True)
        with self.assertRaisesRegex(ValueError, 'multiple members'):
            planner.write_summary(self.root / '{member:.1}.json', summaries)
        self.assertFalse((self.root / '0.json').exists())
        output = self.root / 'read.json'
        for content in ('[]', '{', '{"schema_version": 2, "member": "001"}',
                        '{"schema_version": true, "member": "001"}',
                        '{"001": {"schema_version": 1, "member": "002"}}'):
            with self.subTest(content=content):
                output.write_text(content)
                with self.assertRaises(ValueError):
                    DatasetPlanner.read_summary(output)
        with self.assertRaises(FileNotFoundError):
            DatasetPlanner.read_summary(self.root / 'absent.json')
        with self.assertRaises(TypeError):
            planner.write_summary(output, [])

    def test_summary_selects_only_current_member_and_returns_flat_record(self):
        planner = self.ensemble_planner()
        calls = []

        def current_member_only(path):
            self.assertIn('/run_001/', str(path))
            calls.append(str(path))
            return True

        output = self.root / 'member001.json'
        record = planner.summary(output, member='001', time_mode='forecast', exists=current_member_only)
        self.assertEqual(record['member'], '001')
        self.assertNotIn('001', record)
        self.assertNotIn('002', record)
        self.assertNotIn('members', record)
        self.assertEqual(record['time_steps'], 2)
        self.assertEqual(DatasetPlanner.read_summary(output), record)
        self.assertEqual(len(calls), 2)
        with self.assertRaisesRegex(ValueError, 'member explicitly'):
            planner.summary()
        with self.assertRaisesRegex(ValueError, 'Unknown summary member'):
            planner.summary(member='003')

    def test_summary_execution_merges_completed_records_without_scanning_or_mutating(self):
        planner = self.ensemble_planner()
        records = self.collect(planner, time_mode='forecast', exists=lambda path: True)
        before = deepcopy(records)
        output = self.root / 'execution.json'
        with patch.object(planner, 'summary', side_effect=AssertionError('unexpected member rescan')):
            with patch.object(Path, 'is_file', side_effect=AssertionError('unexpected file probe')):
                merged = planner.summary_execution(records, output, extra_args={'scenario': 'merged'})
        self.assertEqual(set(merged), {'001', '002'})
        for member in merged:
            self.assertEqual(merged[member]['member'], member)
            self.assertEqual(merged[member]['execution']['scenario'], 'merged')
        self.assertEqual(DatasetPlanner.read_summary(output), merged)
        self.assertEqual(records, before)

    def test_deterministic_summary_infers_the_member(self):
        record = self.planner().summary(exists=lambda path: True)
        self.assertEqual(record['member'], 'deterministic')
        self.assertTrue(record['ready'])

    def test_nested_extra_args_keep_allowed_leaves_and_do_not_modify_inputs(self):
        extras = {'001': {'2026-11-13T06:00:00': {
            'status': 'completed', 'seconds': 1.25, 'steps': 3, 'available': True,
            'source': {'rain': '/input/rain.nc', 'invalid': object(), 7: 'bad key'},
            'nan': float('nan'), 'list': [1], 'null': None,
        }}}
        expected = {'001': {'2026-11-13T06:00:00': {
            'status': 'completed', 'seconds': 1.25, 'steps': 3, 'available': True,
            'source': {'rain': '/input/rain.nc'},
        }}}
        planner = self.planner()
        output = self.root / 'nested.json'
        record = planner.summary(output, exists=lambda path: True, extra_args=extras)
        self.assertEqual(record['execution'], expected)
        self.assertEqual(DatasetPlanner.read_summary(output)['execution'], expected)
        self.assertIn('invalid', extras['001']['2026-11-13T06:00:00']['source'])
        self.assertEqual(self.logger.warning.call_count, 5)
        warnings = str(self.logger.warning.call_args_list)
        self.assertIn("extra_args['001']['2026-11-13T06:00:00']['source']['invalid']", warnings)

    def test_nested_cycles_are_skipped_but_shared_dictionaries_are_preserved(self):
        shared = {'status': 'completed'}
        extras = {'first': shared, 'second': shared}
        extras['cycle'] = extras
        cleaned = self.planner()._summary_extra_args(extras)
        self.assertEqual(cleaned, {'first': shared, 'second': shared})
        self.assertIsNot(cleaned['first'], cleaned['second'])
        self.logger.warning.assert_called_once()
        json.dumps(cleaned, allow_nan=False)

    def test_custom_extra_args_tag_survives_writing_and_collection_merging(self):
        planner = self.planner()
        output = self.root / 'custom.json'
        record = planner.summary(output, exists=lambda path: True,
                                 extra_args={'status': 'completed'}, tag_args='diagnostics')
        self.assertEqual(record['diagnostics'], {'status': 'completed'})
        self.assertNotIn('execution', record)
        self.assertNotIn('extra_args', record)
        self.assertEqual(DatasetPlanner.read_summary(output), record)
        written = planner.write_summary(output, record, tag_args='diagnostics', extra_args={'steps': 3})
        self.assertEqual(written['deterministic']['diagnostics'], {'status': 'completed', 'steps': 3})
        merged = planner.summary_execution(written, self.root / 'merged.json')
        self.assertEqual(merged['deterministic']['diagnostics'], {'status': 'completed', 'steps': 3})
        self.assertNotIn('execution', merged['deterministic'])
        self.assertEqual(DatasetPlanner.read_summary(self.root / 'merged.json'), merged)

    def test_extra_args_tags_cannot_overwrite_core_metadata(self):
        planner = self.planner()
        for tag in (None, '', ' ', 7, 'member', 'time_run', 'ready', 'steps'):
            with self.subTest(tag=tag), self.assertRaisesRegex(ValueError, 'tag_args'):
                planner.summary(tag_args=tag)


if __name__ == '__main__':
    unittest.main()
