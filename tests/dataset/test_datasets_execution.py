"""Exercise released sequential profiles with tiny real datasets and JSON handoffs."""
from collections import defaultdict
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd
import xarray as xr

from shybox.config_toolkit.config_handler import ConfigManager
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner


class TestDatasetsExecution(unittest.TestCase):
    def run_profile(self, profile, *, merged=False, compressed=True):
        repository = Path(__file__).resolve().parents[2]
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        environment = dict(os.environ, DOMAIN_NAME='test', PATH_GEO=str(root / 'geo'),
                           PATH_OBS=str(root / 'obs'), PATH_FRC_NWP=str(root / 'nwp'),
                           PATH_FRC_RFARM=str(root / 'rainfarm'), PATH_DST=str(root / 'forcing'),
                           PATH_LOG=str(root / 'logs'), PYTHONDONTWRITEBYTECODE='1',
                           MPLCONFIGDIR=str(root / 'matplotlib'))
        filename = ('app_runner_workflow_s3m_datasets_forcing.json' if profile == 's3m' else
                    f'app_runner_workflow_hmc_datasets_forcing_{profile}.json')
        config = json.loads((repository / 'workflow/runner/datasets' / filename).read_text())
        config['configuration']['variables']['lut']['user'].update(
            time_look_observed='0h', time_look_forecast='2h',
            time_horizon_observed=None, time_horizon_forecast=None,
            time_priority_observed='look', time_priority_forecast='look', ensemble_end=2)
        application = config['configuration']['application']
        application['planning'].update(flag_history=False, flag_adjust_obs_frc=False)
        application.update(summary_merge=merged, summary_extra_args={'scenario': 'tiny'})
        for destination in application['data_destination'].values():
            destination.setdefault('file_args', {})['file_compression'] = compressed
        settings = root / 'settings.json'
        settings.write_text(json.dumps(config))

        # Build the exact input filenames using the released configuration and planner.
        from unittest.mock import patch
        with patch.dict(os.environ, environment):
            manager = ConfigManager.from_source(
                str(settings), root_key='configuration', application_key=None,
                auto_validate=True, auto_fill_lut=True, flat_variables=True, flat_key_mode='value',
                convert_none_to_nan=False, apply_time_template_for_none=False)
            app = manager.fill_obj_from_lut(manager.get_section(section='application'),
                resolve_time_placeholders=False, time_keys=(
                    'time_run', 'time_frequency', 'time_rounding', 'time_look_observed',
                    'time_look_forecast', 'time_horizon_observed', 'time_horizon_forecast',
                    'time_priority_observed', 'time_priority_forecast'))
            tags = DatasetPlanner.create_time_tags(manager.get_section(section='template'),
                                                   manager.get_section(section='format'))
        planner = DatasetPlanner.from_application(app, time_run='2026-11-09 09:35', time_tags=tags)
        files = planner.file_plan.query("group == 'forecast'")
        terrain = app['geo']['terrain']
        terrain_path = Path(terrain['path']) / terrain['file_name']
        terrain_path.parent.mkdir(parents=True, exist_ok=True)
        terrain_path.write_text('ncols 2\nnrows 2\nxllcorner 10\nyllcorner 44\ncellsize 1\nNODATA_value -9999\n1 1\n1 1\n')
        fields_by_file = defaultdict(dict)
        canonical_values = {'rain': 1., 'air_t': 2., 'inc_rad': 3., 'rel_hum': 4., 'wind_speed': 5.}
        expected_rain = {}
        for row in files.itertuples(index=False):
            cfg = app['data_source']['forecast'][row.variable]
            member = str(row.output_member) if pd.notna(row.output_member) else 'deterministic'
            value = canonical_values[row.variable]
            if row.variable == 'rain' and profile == 'probabilistic':
                value += 10 * int(member)
            expected_rain[member] = value if row.variable == 'rain' else expected_rain.get(member, 1.)
            times = sorted(files.time.unique())
            if cfg['dims'] == '3d':
                data = xr.DataArray(np.full((len(times), 2, 2), value),
                    dims=(cfg['time'], cfg['y'], cfg['x']),
                    coords={cfg['time']: times, cfg['y']: [45.5, 44.5], cfg['x']: [10.5, 11.5]})
            else:
                data = xr.DataArray(np.full((2, 2), value), dims=(cfg['y'], cfg['x']),
                    coords={cfg['y']: [45.5, 44.5], cfg['x']: [10.5, 11.5]})
            fields_by_file[row.file_source][row.source_name] = data
        for source, fields in fields_by_file.items():
            source = Path(source)
            source.parent.mkdir(parents=True, exist_ok=True)
            if source.suffix == '.gz':
                plain = source.with_suffix('')
                xr.Dataset(fields).to_netcdf(plain)
                with gzip.open(source, 'wb') as stream:
                    stream.write(plain.read_bytes())
                plain.unlink()
            else:
                xr.Dataset(fields).to_netcdf(source)
        result = subprocess.run(
            [sys.executable, '-B', '-m', 'workflow.runner.datasets.app_runner_workflow_hmc_datasets_main',
             '-settings_file', str(settings), '-time', '2026-11-09 09:35'],
            cwd=repository, env=environment, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout[-5000:])
        summaries = sorted(root.glob('datasets.summary.*.json'))
        members = ('001', '002') if profile == 'probabilistic' else ('deterministic',)
        self.assertEqual(len(summaries), 1 if merged else len(members))
        records = DatasetPlanner.read_summary(summaries[0]) if merged else {
            record['member']: record for record in map(DatasetPlanner.read_summary, summaries)}
        self.assertEqual(set(records), set(members))
        for member, record in records.items():
            self.assertTrue(record['ready'])
            self.assertEqual(record['time_steps'], 2)
            self.assertEqual(record['execution']['scenario'], 'tiny')
            traces = [v for v in record['execution'].values() if isinstance(v, dict)]
            self.assertEqual(len(traces), 2)
            for trace in traces:
                self.assertEqual(trace['status'], 'completed')
                self.assertIn('+00:00', trace['started_at'])
                self.assertTrue(all(trace['destination_exists'].values()))
            for step in record['steps']:
                self.assertEqual(len(step['variables']), 5)
                actual = Path(step['variables']['rain']['file_actual'])
                self.assertEqual(actual.suffix == '.gz', compressed)
                if compressed:
                    plain = root / f'inspection.{member}.nc'
                    with gzip.open(actual, 'rb') as source:
                        plain.write_bytes(source.read())
                else:
                    plain = actual
                with xr.open_dataset(plain) as dataset:
                    for variable, details in step['variables'].items():
                        self.assertIn(details['output_name'], dataset)
                        expected = expected_rain[member] if variable == 'rain' else canonical_values[variable]
                        np.testing.assert_allclose(dataset[details['output_name']], expected)
        self.assertFalse((root / 'plans_file.workspace').exists())

    def test_deterministic_execution_writes_all_fields_and_a_member_summary(self):
        self.run_profile('deterministic', compressed=False)

    def test_ensemble_execution_merges_only_completed_member_records(self):
        self.run_profile('probabilistic', merged=True)

    def test_s3m_execution_uses_s3m_field_names(self):
        self.run_profile('s3m')


if __name__ == '__main__':
    unittest.main()
