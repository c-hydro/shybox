"""Verify timing and progress in the monitoring demo workload."""
import unittest
from unittest.mock import call, patch

from workflow.monitoring import app_monitoring_main_example as example


class TestApplicationSteps(unittest.TestCase):
    def test_logs_actual_steps_and_sleeps_only_for_remaining_time(self):
        with patch.object(example.time, 'sleep') as sleep, patch.object(example.logging, 'info') as info:
            example.run_application(step_seconds=2, total_seconds=5)

        self.assertEqual(sleep.call_args_list, [call(2), call(2), call(1)])
        self.assertEqual(info.call_args_list, [call('Application step %s/%s', step, 3) for step in (1, 2, 3)])

    def test_exact_fractional_duration_has_no_extra_step(self):
        with patch.object(example.time, 'sleep') as sleep:
            example.run_application(step_seconds=0.01, total_seconds=0.07)

        self.assertEqual(sleep.call_args_list, [call(0.01)] * 7)
