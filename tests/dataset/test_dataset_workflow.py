"""Real DataLocal/orchestrator setup from adjusted planner variables and paths."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import configure_dataset_workflows


class TestDatasetWorkflow(unittest.TestCase):
    def setUp(self):
        self.folder = self.enterContext(tempfile.TemporaryDirectory())
        self.logger = Mock()
        time = dict(
            time_run='2026-11-09 09:00', time_rounding='1h', time_frequency='1h',
            hour_reference_observed=None, hour_reference_forecast='00:00',
            time_look_observed='0h', time_horizon_observed=None,
            time_look_forecast='1h', time_horizon_forecast=None,
            priority_observed='look', priority_forecast='look',
        )
        source = dict(
            path=self.folder, file_name='obs.{time:%H}.nc', dims='2d',
            x='lon', y='lat', time='time', nodata=-9999, format='netcdf',
            variable={'precipitation': 'rain'}, mode='deterministic', ensemble=None,
            required=True, tolerance_steps=0, missing_policy='error', as_is=False,
        )
        observed = {'rain': source}
        observed['air_t'] = {**source, 'variable': {'temperature': 'air_t'}}
        forecast = deepcopy(observed)
        for cfg in forecast.values():
            cfg.update(dims='3d', file_name='cube.{source_time:%H}.nc')
        destination = {'forcing': dict(
            path=self.folder, file_name='{time_mode}/forcing.{time:%H}.nc',
            format='netcdf', type='hmc', dims='2d',
            variable={'rain': 'Rain', 'air_t': 'Air_Temperature'},
            units={'rain': 'mm', 'air_t': 'C'},
        )}
        self.config = dict(info={}, time=time, data_src_obs=observed, data_src_frc=forecast,
                           data_dst_obs=destination, data_dst_frc=destination)
        self.workflow = {'options': {'io_config': {'mode': 'many_data_files_to_one_file'}},
                         'process_list': {
                             variable.upper() + '_WORKFLOW': [{
                                 'function': 'mask_data_by_limits', 'datasets': {'data': variable},
                                 'results': {'data': output}, 'args': {},
                             }] for variable, output in destination['forcing']['variable'].items()}}

    def planner(self):
        return DatasetPlanner.from_config(**self.config, logger=self.logger)

    def test_actual_orchestrator_contains_every_variable_without_reading_arrays(self):
        planner = self.planner()
        before = deepcopy(self.workflow)
        with patch.object(DataLocal, 'get_data', side_effect=AssertionError('unexpected array read')):
            configured = configure_dataset_workflows(planner, self.workflow)
        step = configured['deterministic']['2026-11-09 10:00:00']
        self.assertEqual(set(step['source']), {'RAIN_DATA_SRC', 'AIR_T_DATA_SRC'})
        self.assertEqual(set(step['destination']), {'RAIN_DATA_DST', 'AIR_T_DATA_DST'})
        source = step['source']['RAIN_DATA_SRC']
        self.assertIsInstance(source, DataLocal)
        self.assertEqual(source.file_name, 'cube.00.nc')
        self.assertEqual(source.file_type, 'grid_3d')
        self.assertEqual(source.time_reference, pd.Timestamp('2026-11-09 10:00'))
        self.assertEqual(source.variable_template['vars_data'], {'precipitation': 'rain'})
        self.assertEqual(step['destination']['RAIN_DATA_DST'].variable_template['vars_data'], {'rain': 'Rain'})
        self.assertEqual(len(step['orchestrator'].processes), 2)
        self.assertEqual(self.workflow, before)
        self.assertFalse(Path(self.folder, 'forecast').exists())

    def test_member_specific_rain_and_shared_temperature_use_adjusted_forecast(self):
        self.config['data_src_frc']['rain'].update(
            mode='probabilistic', ensemble={'start': 1, 'end': 2, 'format': '03d'},
            file_name='cube.{ensemble_member}.{source_time:%H}.nc')
        planner = self.planner()
        planner.update_time(0, None, exists=lambda path: path.name.startswith('cube.'))
        configured = configure_dataset_workflows(planner, self.workflow)
        self.assertEqual(set(configured), {'001', '002'})
        first = configured['001']['2026-11-09 09:00:00']
        second = configured['002']['2026-11-09 09:00:00']
        self.assertEqual(first['source']['RAIN_DATA_SRC'].file_name, 'cube.001.00.nc')
        self.assertEqual(second['source']['RAIN_DATA_SRC'].file_name, 'cube.002.00.nc')
        self.assertEqual(first['source']['AIR_T_DATA_SRC'].file_name, second['source']['AIR_T_DATA_SRC'].file_name)
        self.assertTrue(str(first['destination']['RAIN_DATA_DST'].path).endswith('forecast'))

    def test_missing_variable_process_fails_before_creating_handlers(self):
        del self.workflow['process_list']['AIR_T_WORKFLOW']
        with self.assertRaisesRegex(ValueError, 'AIR_T_WORKFLOW'):
            configure_dataset_workflows(self.planner(), self.workflow)


if __name__ == '__main__':
    unittest.main()
