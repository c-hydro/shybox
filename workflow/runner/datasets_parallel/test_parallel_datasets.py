"""Tests live exclusively in the new folder; original workflow tests are untouched."""
from copy import deepcopy
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from workflow.runner.datasets_parallel.executor import iter_task_results
from workflow.runner.datasets_parallel.pipeline import prepare_run, run_parallel_datasets, stage_source, validate_ownership
from workflow.runner.datasets_parallel.support_workers import scheduler_probe
from workflow.runner.datasets_parallel.verify_originals import changed_originals


class TestProcessScheduling(unittest.TestCase):
    def test_real_spawn_processes_and_worker_errors_return_all_jobs(self):
        tasks = [{'task_id': str(index), 'member': '001', 'value': index, 'fail': index == 3}
                 for index in range(8)]
        original_threads = os.environ.get('OMP_NUM_THREADS')
        results = list(iter_task_results(tasks, scheduler_probe, max_workers=2, native_threads=1))
        self.assertEqual(len(results), 8)
        reports = {task['task_id']: result for task, result in results}
        self.assertEqual(reports['3']['status'], 'failed')
        self.assertIn('intentional worker failure', reports['3']['error'])
        self.assertEqual(reports['7']['value'], 14)
        successful = [result for _, result in results if result['status'] == 'completed']
        self.assertEqual(len({result['pid'] for result in successful}), 2)
        self.assertNotIn(os.getpid(), {result['pid'] for result in successful})
        self.assertTrue(all(result['native_threads'] == '1' for result in successful))
        self.assertEqual(os.environ.get('OMP_NUM_THREADS'), original_threads)

    def test_invalid_worker_counts_are_rejected(self):
        with self.assertRaises(ValueError):
            list(iter_task_results([], scheduler_probe, max_workers=0))

    def test_worker_crash_reports_pending_and_unsubmitted_tasks(self):
        tasks = [{'task_id': str(index), 'member': '001', 'crash': index == 0} for index in range(12)]
        results = list(iter_task_results(tasks, scheduler_probe, max_workers=2))
        self.assertEqual(len(results), len(tasks))
        self.assertEqual({task['task_id'] for task, _ in results}, {task['task_id'] for task in tasks})
        self.assertTrue(any(result['status'] == 'failed' for _, result in results))


class TestParallelDatasetPreparation(unittest.TestCase):
    def setUp(self):
        import numpy as np
        import pandas as pd
        import xarray as xr
        self.np, self.pd, self.xr = np, pd, xr
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.input = self.root / 'input'; self.input.mkdir()
        self.geo = self.input / 'domain.dem.txt'
        self.geo.write_text('ncols 2\nnrows 2\nxllcorner 10\nyllcorner 44\ncellsize 1\nNODATA_value -9999\n1 1\n1 1\n')
        self.times = pd.date_range('2026-11-09 10:00', periods=2, freq='h')
        self.variables = {'rain': 'Rain', 'air_t': 'Air_Temperature', 'inc_rad': 'Incoming_Radiation',
                          'rel_hum': 'Relative_Humidity', 'wind_speed': 'Wind'}
        data = {name: (('time', 'latitude', 'longitude'), np.full((2, 2, 2), index + 1., dtype=float))
                for index, name in enumerate(self.variables)}
        coordinates = {'time': self.times, 'latitude': [45.5, 44.5], 'longitude': [10.5, 11.5]}
        xr.Dataset(data, coords=coordinates).to_netcdf(self.input / 'shared.nc')
        for member in ('001', '002'):
            rain = np.full((2, 2, 2), 10. + int(member), dtype=float)
            xr.Dataset({'rain': (('time', 'latitude', 'longitude'), rain)}, coords=coordinates).to_netcdf(
                self.input / f'rain.{member}.nc')
        source = {'path': str(self.input), 'file_name': 'shared.nc', 'x': 'longitude', 'y': 'latitude',
                  'time': 'time', 'dims': '3d', 'nodata': -9999, 'format': 'netcdf',
                  'mode': 'deterministic', 'ensemble': None, 'required': True,
                  'tolerance_steps': 0, 'missing_policy': 'error', 'as_is': False}
        sources = {name: {**source, 'variable': {name: name}} for name in self.variables}
        observed = deepcopy(sources)
        for config in observed.values(): config.update(file_name='observed.nc', dims='2d')
        self.application = {
            'info': {'model': 'hmc', 'domain_name': 'domain'},
            'time': {'time_run': '2026-11-09 09:00', 'time_rounding': '1h', 'time_frequency': '1h',
                     'hour_reference_observed': None, 'hour_reference_forecast': '00:00',
                     'time_look_observed': '0h', 'time_horizon_observed': None,
                     'time_look_forecast': '2h', 'time_horizon_forecast': None,
                     'priority_observed': 'look', 'priority_forecast': 'look'},
            'geo': {'terrain': {'path': str(self.input), 'file_name': self.geo.name, 'format': 'ascii'}},
            'data_source': {'observed': observed, 'forecast': sources},
            'data_destination': {'forcing': {
                'path': str(self.root / 'ORIGINAL_OUTPUT_MUST_NOT_BE_USED'),
                'file_name': 'forcing.{time:%Y%m%d%H}.nc', 'format': 'netcdf', 'type': 'hmc_forcing',
                'dims': '2d', 'variable': self.variables, 'units': {name: 'test' for name in self.variables}}},
            'planning': {'flag_adjust_obs_frc': False, 'n_step_to_check_obs': None,
                         'n_step_to_check_frc': None},
        }
        self.workflow = {'options': {'io_config': {'mode': 'many_data_files_to_one_file'},
                                     'intermediate_output': 'Memory'},
                         'process_list': {name.upper() + '_WORKFLOW': [{
                             'function': 'mask_data_by_limits', 'datasets': {'data': name},
                             'results': {'data': output}, 'args': {'mask_min': None, 'mask_max': None,
                                                                  'mask_format': None}}]
                             for name, output in self.variables.items()}}
        self.input_hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in self.input.iterdir()}

    def prepare(self, probabilistic=False, **kwargs):
        if probabilistic:
            self.application['data_source']['forecast']['rain'].update(
                mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
                file_name='rain.{ensemble_member}.nc')
        return prepare_run(self.application, self.workflow, run_dir=self.root / 'parallel_run', **kwargs)

    def test_planning_isolated_and_does_not_read_arrays_or_modify_input_configuration(self):
        before_application, before_workflow = deepcopy(self.application), deepcopy(self.workflow)
        with patch.object(self.xr, 'open_dataset', side_effect=AssertionError('unexpected array read')):
            prepared = self.prepare()
        self.assertEqual(self.application, before_application)
        self.assertEqual(self.workflow, before_workflow)
        self.assertEqual(prepared.task_unit, 'time')
        self.assertEqual(len(prepared.tasks), 2)
        self.assertFalse((prepared.run_dir / 'forcing').exists())
        self.assertFalse((self.root / 'ORIGINAL_OUTPUT_MUST_NOT_BE_USED').exists())
        self.assertTrue((prepared.run_dir / 'plan.json').is_file())
        with self.assertRaises(FileExistsError):
            prepare_run(self.application, self.workflow, run_dir=prepared.run_dir)

    def test_probabilistic_planning_assigns_one_member_per_job(self):
        prepared = self.prepare(probabilistic=True)
        self.assertEqual(prepared.task_unit, 'member')
        self.assertEqual([task['member'] for task in prepared.tasks], ['001', '002'])
        self.assertTrue(all(len(task['steps']) == 2 for task in prepared.tasks))
        paths = [var['source_path'] for var in prepared.tasks[0]['steps'][0]['variables'] if var['name'] == 'air_t']
        self.assertEqual(paths, [str(self.input / 'shared.nc')])

    def test_output_aliases_and_path_escape_are_rejected_before_workers_start(self):
        task = {'member': '001', 'steps': [{'time': 'time1', 'variables': [
            {'source_path': str(self.input / 'shared.nc'), 'destination_path': str(self.root / 'run' / 'file.nc')}]}]}
        other = deepcopy(task); other['member'] = '002'
        other['steps'][0]['variables'][0]['destination_path'] += '.gz'
        with self.assertRaisesRegex(ValueError, 'shared'):
            validate_ownership([task, other], self.root / 'run')
        with self.assertRaisesRegex(ValueError, 'escapes'):
            validate_ownership([task], self.root / 'different_root')

    def check_real_outputs(self, prepared, report, probabilistic):
        self.assertEqual(report['status'], 'completed', json.dumps(report, indent=2))
        self.assertTrue(report['ready'])
        self.assertFalse((self.root / 'ORIGINAL_OUTPUT_MUST_NOT_BE_USED').exists())
        merged = json.loads(Path(report['merged_summary']).read_text())
        expected_members = ('001', '002') if probabilistic else ('deterministic',)
        self.assertEqual(set(merged), set(expected_members))
        for member in expected_members:
            summary = merged[member]
            self.assertEqual(summary['member'], member)
            self.assertEqual(summary['time_steps'], 2)
            self.assertTrue(summary['ready'])
            for step in summary['steps']:
                output = step['variables']['rain']['file_actual']
                readable = stage_source(output, self.root / 'inspection', 'netcdf')
                with self.xr.open_dataset(readable, engine='netcdf4') as data:
                    self.assertTrue(set(self.variables.values()) <= set(data.data_vars))
                    expected_rain = 10. + int(member) if probabilistic else 1.
                    self.np.testing.assert_allclose(data.Rain.values, expected_rain)
                    self.np.testing.assert_allclose(data.Air_Temperature.values, 2.)
                    self.np.testing.assert_allclose(data.Wind.values, 5.)
        self.assertTrue(all(result['pid'] != os.getpid() for result in report['tasks'].values()))
        for path, expected in self.input_hashes.items():
            self.assertEqual(hashlib.sha256(Path(path).read_bytes()).hexdigest(), expected)

    def test_real_deterministic_outputs_in_spawned_processes(self):
        prepared = self.prepare()
        report = run_parallel_datasets(prepared, max_workers=2)
        self.check_real_outputs(prepared, report, False)

    def test_real_probabilistic_outputs_include_shared_deterministic_fields(self):
        prepared = self.prepare(probabilistic=True)
        report = run_parallel_datasets(prepared, max_workers=2)
        self.check_real_outputs(prepared, report, True)

    def test_compressed_inputs_are_staged_privately_and_plain_originals_are_preserved(self):
        for path in list(self.input.glob('*.nc')):
            with path.open('rb') as source, gzip.open(str(path) + '.gz', 'wb') as output:
                output.write(source.read())
        for source in self.application['data_source']['forecast'].values():
            source['file_name'] = 'shared.nc.gz'
        self.application['data_source']['forecast']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='rain.{ensemble_member}.nc.gz')
        self.input_hashes.update({str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in self.input.glob('*.gz')})
        prepared = self.prepare()
        report = run_parallel_datasets(prepared, max_workers=2)
        self.check_real_outputs(prepared, report, True)
        self.assertTrue(list((prepared.run_dir / 'tmp').glob('*/sources/*/shared.nc')))

    def test_missing_member_input_is_reported_without_stopping_other_members(self):
        prepared = self.prepare(probabilistic=True)
        (self.input / 'rain.002.nc').unlink()
        report = run_parallel_datasets(prepared, max_workers=2)
        self.assertEqual(report['status'], 'failed')
        merged = json.loads(Path(report['merged_summary']).read_text())
        self.assertTrue(merged['001']['ready'])
        self.assertFalse(merged['002']['ready'])
        failures = [result for result in report['tasks'].values() if result['status'] == 'failed']
        self.assertEqual(len(failures), 1)
        self.assertIn('FileNotFoundError', failures[0]['error'])


class TestExistingProfiles(unittest.TestCase):
    def test_cli_dry_run_accepts_both_existing_profiles_without_writing_their_outputs(self):
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', DOMAIN_NAME='domain',
                       PATH_GEO=str(folder), PATH_OBS=str(folder), PATH_FRC_NWP=str(folder),
                       PATH_FRC_RFARM=str(folder), PATH_DST=str(folder / 'old_output'),
                       PATH_LOG=str(folder / 'old_logs'), PATH_TMP=str(folder / 'old_tmp'))
            for mode in ('deterministic', 'probabilistic'):
                original = root / 'workflow/runner/datasets' / f'app_runner_workflow_hmc_datasets_forcing_{mode}.json'
                config = json.loads(original.read_text())
                lut = config['configuration']['variables']['lut']['user']
                lut.update(time_look_observed='0h', time_look_forecast='1h',
                           time_horizon_observed=None, time_horizon_forecast=None,
                           time_priority_observed='look', time_priority_forecast='look', ensemble_end=2)
                settings = folder / f'{mode}.json'; settings.write_text(json.dumps(config))
                run_dir = folder / f'{mode}_run'
                result = subprocess.run([
                    sys.executable, '-B', '-m', 'workflow.runner.datasets_parallel.app_runner_workflow_hmc_datasets_parallel',
                    '--settings-file', str(settings), '--time', '2026-11-09 09:35',
                    '--run-dir', str(run_dir), '--dry-run'], cwd=root, env=env,
                    capture_output=True, text=True, timeout=45)
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout[-2500:])
                plan = json.loads((run_dir / 'plan.json').read_text())
                self.assertEqual(plan['members'], ['deterministic'] if mode == 'deterministic' else ['001', '002'])
                self.assertFalse((run_dir / 'forcing').exists())
                self.assertFalse((folder / 'old_output').exists())
                self.assertFalse((folder / 'old_logs').exists())
                self.assertFalse((folder / 'old_tmp').exists())

    def test_preexisting_tracked_files_remain_byte_identical(self):
        self.assertEqual(changed_originals(), [])


if __name__ == '__main__':
    unittest.main()
