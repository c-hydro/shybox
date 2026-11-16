"""Real HMC mode execution, independent member updates and reusable shared files."""
from collections import defaultdict
from copy import deepcopy
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
from shybox.runner_toolkit.datasets.lib_utils_mode import file_digest, publish_member
from workflow.runner.datasets_mode import app_runner_workflow_hmc_datasets_main as runner


class TestDatasetModes(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repository = Path(__file__).resolve().parents[2]
        environment = {key: str(self.root / key.lower()) for key in (
            'PATH_GEO', 'PATH_OBS', 'PATH_FRC_NWP', 'PATH_FRC_RFARM', 'PATH_DST',
            'PATH_LOG', 'PATH_TMP', 'PATH_PLANNING', 'PATH_SUMMARY')}
        environment['DOMAIN_NAME'] = 'test'
        self.enterContext(patch.dict(os.environ, environment))
        LoggingManager.setup(handlers=['stream'], force_reconfigure=True)
        self.logger = LoggingManager(name='test_modes', level=40, set_as_current=True)

    def prepare(self, profile='probabilistic', all_probabilistic=False):
        original = self.repository / 'workflow/runner/datasets' / (
            f'app_runner_workflow_hmc_datasets_forcing_{profile}.json')
        cfg = json.loads(original.read_text())
        user = cfg['configuration']['variables']['lut']['user']
        user.update(time_look_observed='0h', time_look_forecast='2h',
                    time_horizon_observed=None, time_horizon_forecast=None)
        if profile == 'probabilistic':
            user['ensemble_end'] = 2
        if all_probabilistic:
            for source in cfg['configuration']['application']['data_source']['forecast'].values():
                source.update(mode='probabilistic',
                              ensemble={'start': 1, 'end': 2, 'format': '03d'},
                              file_name='all.{file_time_forecast_nwp}.{ensemble_member}.nc')
        settings = self.root / 'settings.json'
        settings.write_text(json.dumps(cfg))
        app, workflow, tags = runner.load_configuration(settings)
        planning = app['planning']
        planner = DatasetPlanner.from_config(
            info=app['info'], time=app['time'], time_run='2026-11-09 09:00',
            data_src_obs=app['data_source']['observed'], data_src_frc=app['data_source']['forecast'],
            data_dst_obs=app['data_destination'], data_dst_frc=app['data_destination'],
            geo=app['geo'], time_tags=tags, logger=self.logger,
            options=planning['options'], files=planning['files'], summary=planning['summary'])
        terrain = app['geo']['terrain']
        terrain_path = Path(terrain['path']) / terrain['file_name']
        terrain_path.parent.mkdir(parents=True, exist_ok=True)
        terrain_path.write_text('ncols 2\nnrows 2\nxllcorner 10\nyllcorner 44\ncellsize 1\nNODATA_value -9999\n1 1\n1 1\n')
        files = planner.file_plan
        fields = defaultdict(dict)
        values = {'rain': 1., 'air_t': 2., 'inc_rad': 3., 'rel_hum': 4., 'wind_speed': 5.}
        for row in files.itertuples(index=False):
            source = planner.source_settings(row.variable, row.time_mode)
            value = values[row.variable]
            if row.time_mode == 'observed':
                value += 100
            elif row.mode != 'deterministic':
                value += 10 * int(row.ensemble_member)
            if source['dims'] == '3d':
                times = sorted(files.loc[files.file_source.eq(row.file_source), 'time'].unique())
                data = xr.DataArray(np.full((len(times), 2, 2), value),
                    dims=('time', 'latitude', 'longitude'),
                    coords={'time': times, 'latitude': [45.5, 44.5], 'longitude': [10.5, 11.5]})
            else:
                data = xr.DataArray(np.full((2, 2), value), dims=('latitude', 'longitude'),
                    coords={'latitude': [45.5, 44.5], 'longitude': [10.5, 11.5]})
            fields[row.file_source][row.source_name] = data
        for filename, variables in fields.items():
            path = Path(filename)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.suffix == '.gz':
                with gzip.open(path, 'wb') as stream:
                    stream.write(xr.Dataset(variables).to_netcdf())
            else:
                xr.Dataset(variables).to_netcdf(path)
        planner.update_time(flag_history=False)
        return planner, workflow

    def test_shared_fields_are_computed_once_then_copied_and_reused(self):
        planner, workflow = self.prepare()
        workspace = self.root / 'workspace'
        original_plan = planner.file_plan
        inputs = {path: file_digest(path) for path in original_plan.file_source.unique()}
        with patch.object(runner, 'process_step', wraps=runner.process_step) as process:
            result = runner.run(planner, workflow, workspace, self.logger)
        self.assertEqual(result['deterministic_computed'], 3)
        self.assertEqual(result['deterministic_reused'], 0)
        self.assertEqual(process.call_count, 7)  # 3 shared + 2 times * 2 members.
        self.assertEqual(len(result['published_files']), 6)
        for member, record in result['summaries'].items():
            self.assertTrue(record['ready'])
            self.assertEqual(record['time_steps'], 3)
            for step in record['steps']:
                output = Path(step['variables']['rain']['file_actual'])
                with gzip.open(output, 'rb') as stream:
                    inspection = self.root / 'inspection.nc'
                    inspection.write_bytes(stream.read())
                with xr.open_dataset(inspection) as dataset:
                    self.assertEqual(len(set(dataset.data_vars) & {'Rain', 'Air_Temperature',
                        'Incoming_Radiation', 'Relative_Humidity', 'Wind'}), 5)
                    offset = 100 if step['time_mode'] == 'observed' else 0
                    np.testing.assert_allclose(dataset['Air_Temperature'], 2 + offset)
                    np.testing.assert_allclose(dataset['Wind'], 5 + offset)
                    self.assertEqual(dataset['Air_Temperature'].attrs['units'], 'C')
                    expected_rain = 101 if offset else 1 + 10 * int(member)
                    np.testing.assert_allclose(dataset['Rain'], expected_rain)
        cache = {str(path): file_digest(path) for path in (workspace / 'deterministic').glob('*.nc')}
        with patch.object(runner, 'process_step', wraps=runner.process_step) as process:
            repeated = runner.run(planner, workflow, workspace, self.logger)
        self.assertEqual(repeated['deterministic_computed'], 0)
        self.assertEqual(repeated['deterministic_reused'], 3)
        self.assertEqual(process.call_count, 4)  # Only member fields are processed again.
        self.assertEqual(cache, {path: file_digest(path) for path in cache})
        self.assertEqual(inputs, {path: file_digest(path) for path in inputs})
        for path in inputs:
            if path.endswith('.gz'):
                self.assertFalse(Path(path[:-3]).exists())
        pd.testing.assert_frame_equal(original_plan, planner.file_plan)

        # A damaged cached file is rebuilt instead of being copied to members.
        cached = next((workspace / 'deterministic').glob('*.nc'))
        cached.write_bytes(b'interrupted cache write')
        repaired = runner.run(planner, workflow, workspace, self.logger)
        self.assertGreaterEqual(repaired['deterministic_computed'], 1)

        # A changed deterministic cube invalidates only its two shared times.
        cube = next(path for path in inputs if 'nwp.ecmwf' in path)
        stat = Path(cube).stat()
        os.utime(cube, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
        changed = runner.run(planner, workflow, workspace, self.logger)
        self.assertEqual(changed['deterministic_computed'], 2)
        self.assertEqual(changed['deterministic_reused'], 1)

    def test_mode_filters_deduplicate_shared_rows_without_mutating_plan(self):
        planner, _ = self.prepare()
        before = planner.file_plan
        shared = planner.file_plan_by_mode('deterministic')
        self.assertEqual(len(shared), 5 + 2 * 4)
        self.assertEqual(len(planner.file_plan_by_mode('probabilistic')), 4)
        self.assertEqual(len(planner.file_plan_by_mode('probabilistic', member='001')), 2)
        self.assertEqual(len(planner.file_plan_by_mode('deterministic', time_mode='observed')), 5)
        cfg = planner.source_settings('rain', 'observed')
        cfg['variable'].clear()
        self.assertTrue(planner.source_settings('rain', 'observed')['variable'])
        pd.testing.assert_frame_equal(before, planner.file_plan)
        with self.assertRaises(ValueError):
            planner.file_plan_by_mode('unknown')

    def test_deterministic_only_and_probabilistic_only_forecasts(self):
        for profile, all_probabilistic in (('deterministic', False), ('probabilistic', True)):
            with self.subTest(profile=profile):
                planner, workflow = self.prepare(profile, all_probabilistic=all_probabilistic)
                result = runner.run(planner, workflow, self.root / ('workspace_' + profile), self.logger,
                                    time_mode='forecast')
                self.assertEqual(result['deterministic_computed'], 0 if all_probabilistic else 2)
                self.assertTrue(all(record['ready'] for record in result['summaries'].values()))

    def test_failed_member_update_preserves_old_output_and_shared_file(self):
        base, update, output = (self.root / name for name in ('base.nc', 'update.nc', 'member.nc'))
        xr.Dataset({'Shared': ('x', [2., 3.])}, coords={'x': [0, 1]}).to_netcdf(base)
        xr.Dataset({'Rain': ('x', [8., 9., 10.])}, coords={'x': [0, 1, 2]}).to_netcdf(update)
        output.write_bytes(b'previous member output')
        digest = file_digest(base)
        with self.assertRaisesRegex(ValueError, 'Incompatible update dimension'):
            publish_member(base, [(update, ['Rain'])], output, compress=False)
        self.assertEqual(output.read_bytes(), b'previous member output')
        self.assertEqual(file_digest(base), digest)

    def test_new_cli_uses_existing_profile_without_editing_it(self):
        self.prepare()
        settings = self.root / 'settings.json'
        before = settings.read_bytes()
        workspace = self.root / 'cli_workspace'
        result = subprocess.run([
            sys.executable, '-B', '-m', 'workflow.runner.datasets_mode.app_runner_workflow_hmc_datasets_main',
            '-settings_file', str(settings), '-time', '2026-11-09 09:00', '--workspace', str(workspace)],
            cwd=self.repository, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout[-3000:])
        self.assertEqual(settings.read_bytes(), before)
        self.assertEqual(len(list((workspace / 'deterministic').glob('*.nc'))), 3)
        self.assertTrue((workspace / 'planning.workspace').is_file())
        summaries = list((self.root / 'path_summary').rglob('*.json'))
        self.assertEqual(len(summaries), 2)
        self.assertTrue(all(json.loads(path.read_text())['ready'] for path in summaries))

    def test_colliding_member_outputs_are_rejected_before_processing(self):
        planner, workflow = self.prepare()
        planner._file_plan['file_destination'] = str(self.root / 'same.nc')
        with patch.object(runner, 'process_step') as process, self.assertRaisesRegex(ValueError, 'shared'):
            runner.run(planner, workflow, self.root / 'workspace', self.logger)
        process.assert_not_called()
