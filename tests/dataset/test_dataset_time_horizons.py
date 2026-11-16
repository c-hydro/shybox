"""Look-window fallbacks and explicit clock-time horizon validation."""
import unittest

import pandas as pd

from shybox.runner_toolkit.datasets.lib_utils_datasets import create_time_period


class TestDatasetTimeHorizons(unittest.TestCase):
    def period(self, **kwargs):
        config = dict(time_run='2026-11-09 14:00', time_look_observed='3h',
                      time_look_forecast='3h', time_horizon_observed=None,
                      time_horizon_forecast=None)
        config.update(kwargs)
        return create_time_period(**config)

    def test_null_horizons_use_look_for_every_priority(self):
        for priority in ('look', 'horizon', 'min', 'max'):
            with self.subTest(priority=priority):
                plan = self.period(priority_observed=priority, priority_forecast=priority)
                self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-09 11:00'))
                self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-09 17:00'))

    def test_midnight_keeps_existing_day_boundary_policy(self):
        plan = self.period(time_horizon_observed='midnight')
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-09 00:00'))

    def test_clock_horizons_are_exact_same_day_boundaries(self):
        for priority in ('look', 'horizon', 'min', 'max'):
            with self.subTest(priority=priority):
                plan = self.period(time_horizon_observed='12:00', time_horizon_forecast='15:00',
                                   priority_observed=priority, priority_forecast=priority)
                self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-09 12:00'))
                self.assertEqual(plan.forecast[-1], pd.Timestamp('2026-11-09 15:00'))

    def test_clock_horizons_outside_look_window_explain_configuration_error(self):
        for group, clock in (('observed', '15:00'), ('observed', '10:00'),
                             ('forecast', '12:00'), ('forecast', '18:00')):
            with self.subTest(group=group, clock=clock):
                with self.assertRaisesRegex(ValueError, f'Cannot use time_horizon_{group}.*must be within'):
                    self.period(**{f'time_horizon_{group}': clock})

    def test_clock_horizon_includes_look_and_run_boundaries(self):
        for clock in ('11:00', '14:00'):
            plan = self.period(time_horizon_observed=clock)
            self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-09 ' + clock))
        plan = self.period(time_run='2026-11-09 01:00', time_horizon_observed='00:00')
        self.assertEqual(plan.observed[0], pd.Timestamp('2026-11-09 00:00'))

    def test_clock_format_and_frequency_are_validated(self):
        for clock in ('24:00', '12:60', '1:00', '12:00:00', '12:30'):
            with self.subTest(clock=clock), self.assertRaisesRegex(ValueError, 'time_horizon_observed'):
                self.period(time_horizon_observed=clock)
