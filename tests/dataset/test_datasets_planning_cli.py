"""Resolve shipped profiles and nested planning outputs without executing arrays."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from shybox.config_toolkit.config_handler import ConfigManager
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner


class TestDatasetsPlanningCLI(unittest.TestCase):
    def test_current_profiles_resolve_planning_and_summary_paths(self):
        repository = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as folder:
            environment = {name: folder for name in (
                'PATH_GEO', 'PATH_OBS', 'PATH_FRC_NWP', 'PATH_FRC_RFARM',
                'PATH_DST', 'PATH_LOG', 'PATH_TMP', 'PATH_PLANNING', 'PATH_SUMMARY')}
            environment['DOMAIN_NAME'] = 'test'
            for profile in ('hmc_datasets_forcing_deterministic',
                            'hmc_datasets_forcing_probabilistic', 's3m_datasets_forcing'):
                with self.subTest(profile=profile), patch.dict(os.environ, environment):
                    settings = repository / 'workflow/runner/datasets' / ('app_runner_workflow_' + profile + '.json')
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
                    planner = DatasetPlanner.from_application(
                        app, time_run='2026-11-09 10:00', time_tags=tags,
                        options=app['planning']['options'], files=app['planning']['files'],
                        summary=app['planning']['summary'])
                    planner.update_time(flag_history=False, exists=lambda path: True)
                    self.assertEqual(int(planner.time_table.time_checked.sum()), 5)
                    members = planner.members or ('deterministic',)
                    destinations = set()
                    for member in members:
                        planner.summary(member=member, exists=lambda path: True)
                        self.assertTrue(Path(planner.file_summary).is_file())
                        self.assertNotIn('{', planner.file_summary)
                        destinations.add(planner.file_summary)
                    self.assertEqual(len(destinations), len(members))
                    self.assertFalse(list(Path(folder).rglob('*.workspace')))
