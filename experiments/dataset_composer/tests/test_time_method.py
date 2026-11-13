"""Look/horizon boundary priorities and step/cube reference tables."""
import json
import unittest

import pandas as pd

from shybox.runner_toolkit.datasets.lib_utils_datasets import create_time_period
from shybox.runner_toolkit.datasets.datasets_handler import time_configure


class TestTimeMethod(unittest.TestCase):
    def test_requested_settings_and_whitespace_rounding(self):
        plan = create_time_period('2026-11-09 09:35', time_rounding=' 1h')
        self.assertEqual(plan.time_run, pd.Timestamp('2026-11-09 09:00'))
        self.assertEqual(plan.forecast_run, pd.Timestamp('2026-11-09 00:00'))
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-07 00:00'))
        self.assertEqual(plan.observed[-1], plan.time_run)
        self.assertEqual(plan.forecast[0], pd.Timestamp('2026-11-09 10:00'))
        self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-12 00:00'))
        self.assertEqual((len(plan.observed), len(plan.forecast), len(plan.time_period)), (58, 63, 121))
        self.assertTrue(plan.time_period.is_unique)
        self.assertTrue(plan.time_period.is_monotonic_increasing)

    def test_independent_min_max_priorities(self):
        expected_starts = {'min': '2026-11-07 00:00', 'max': '2026-11-07 09:00',
                           'look': '2026-11-07 09:00', 'horizon': '2026-11-07 00:00'}
        expected_ends = {'min': '2026-11-12 00:00', 'max': '2026-11-12 09:00',
                         'look': '2026-11-12 09:00', 'horizon': '2026-11-12 00:00'}
        for obs_priority, start in expected_starts.items():
            for frc_priority, end in expected_ends.items():
                with self.subTest(observed=obs_priority, forecast=frc_priority):
                    plan = create_time_period('2026-11-09 09:35', priority_observed=obs_priority,
                                              priority_forecast=frc_priority)
                    self.assertEqual(plan.observed[0], pd.Timestamp(start))
                    self.assertEqual(plan.forecast[-1], pd.Timestamp(end))

    def test_step_and_cube_reference_columns(self):
        plan = create_time_period('2026-11-09 09:35')
        obs, frc = pd.Timestamp('2026-11-09 09:00'), pd.Timestamp('2026-11-10 15:00')
        self.assertEqual(plan.table.loc[obs, 'time_mode'], 'obs')
        self.assertEqual(plan.table.loc[frc, 'time_mode'], 'frc')
        self.assertEqual(plan.table.loc[obs, 'time_reference'], obs)
        self.assertEqual(plan.table.loc[frc, 'time_reference'], pd.Timestamp('2026-11-09 00:00'))
        self.assertEqual(plan.reference_time(frc, 'step'), frc)
        self.assertEqual(plan.reference_time(frc, '3d'), pd.Timestamp('2026-11-09 00:00'))
        self.assertEqual(plan.table.loc[obs, 'time_step'], 0)
        self.assertEqual(plan.table.loc[obs - pd.Timedelta('2h'), 'time_step'], -2)
        self.assertEqual(plan.table.loc[obs + pd.Timedelta('2h'), 'time_step'], 2)
        self.assertEqual(plan.table.loc[plan.forecast, 'file_id'].nunique(), 1)
        self.assertEqual(plan.table.loc[plan.observed, 'file_id'].nunique(), len(plan.observed))
        self.assertEqual(plan.table.file_id.nunique(), len(plan.observed) + 1)
        self.assertIsInstance(plan.as_dict()['time_table'][0]['time_step'], int)

    def test_reference_modes_can_be_selected_independently(self):
        plan = create_time_period('2026-11-09 09:35', hour_reference_observed='06:00',
                                  reference_mode_observed='3d', reference_mode_forecast='step')
        self.assertTrue((plan.table.loc[plan.observed, 'time_reference'] == pd.Timestamp('2026-11-09 06:00')).all())
        self.assertEqual(list(plan.table.loc[plan.forecast, 'time_reference']), list(plan.forecast))

    def test_future_clock_selects_previous_cycle(self):
        plan = create_time_period('2026-11-09 09:35', hour_reference_forecast='12:00')
        self.assertEqual(plan.forecast_run, pd.Timestamp('2026-11-08 12:00'))

    def test_disabled_horizons_use_look_boundaries(self):
        plan = create_time_period('2026-11-09 09:35', time_horizon_observed=None, time_horizon_forecast=None)
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-07 09:00'))
        self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-12 09:00'))

    def test_fractional_look_aligns_to_forcing_steps(self):
        plan = create_time_period('2026-11-09 09:35', time_look_observed='1h30min', time_horizon_observed=None,
                                  time_look_forecast='1h30min', time_horizon_forecast=None)
        self.assertEqual(list(plan.observed), list(pd.date_range('2026-11-09 08:00', periods=2, freq='h')))
        self.assertEqual(list(plan.forecast), [pd.Timestamp('2026-11-09 10:00')])

    def test_table_and_boundaries_are_json_serializable(self):
        plan = create_time_period('2026-11-09 09:35')
        result = json.loads(json.dumps(plan.as_dict()))
        self.assertEqual(result['bounds']['forecast']['priority'], 'min')
        self.assertEqual(result['time_table'][-1]['time_mode'], 'frc')

    def test_invalid_time_policies_and_reference_hours_fail(self):
        cases = [{'priority_observed': 'bad'}, {'hour_reference_forecast': '24:00'},
                 {'hour_reference_observed': '12:70'}, {'time_look_forecast': '-1h'},
                 {'time_frequency': '0h'}, {'reference_mode_forecast': 'bad'},
                 {'priority_forecast': 'horizon', 'time_horizon_forecast': None}]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                create_time_period('2026-11-09 09:35', **kwargs)

    def test_handler_accepts_lut_and_priority_overrides(self):
        plan = time_configure({'time_run': '2026-11-09 09:35', 'time_rounding': ' 1h',
                               'time_priority_observed': 'min', 'time_priority_forecast': 'min',
                               'path_observed': '/unused/path'}, priority_forecast='max')
        self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-12 09:00'))
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-07 00:00'))

    def test_handler_accepts_explicit_args_and_source_reference_modes(self):
        plan = time_configure(time_run='2026-11-09 09:35', time_look_forecast='2h',
                              time_horizon_forecast=None)
        self.assertEqual(len(plan.forecast), 2)
        source_settings = {'time': {'time_run': '2026-11-09 09:35', 'time_look_observed': '48h'},
                           'sources': {'observed': {}, 'forecast': {'rain': {'operations': {}}}}}
        plan = time_configure(source_settings)
        self.assertEqual(list(plan.table.loc[plan.forecast, 'time_reference']), list(plan.forecast))


if __name__ == '__main__':
    unittest.main()
