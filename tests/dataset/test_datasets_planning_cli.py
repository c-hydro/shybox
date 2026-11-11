"""Exercise deferred LUT dates and availability reporting through the real CLI."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class TestDatasetsPlanningCLI(unittest.TestCase):
    def test_example_profiles_plan_each_step_without_creating_outputs(self):
        repository = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            environment = dict(os.environ, DOMAIN_NAME='marche', PATH_GEO=folder,
                               PATH_OBS=folder, PATH_FRC_NWP=folder, PATH_FRC_RFARM=folder,
                               PATH_DST=str(root / 'forcing'), PATH_LOG=str(root / 'logs'))
            for profile, sources, outputs in (('deterministic', 2, 2), ('probabilistic', 4, 4), ('s3m', 10, 2)):
                with self.subTest(profile=profile):
                    settings = repository / 'workflow/runner/datasets' / (
                        'app_runner_workflow_s3m_datasets_forcing.json' if profile == 's3m' else
                        f'app_runner_workflow_hmc_datasets_forcing_{profile}.json')
                    config = json.loads(settings.read_text())
                    lut = config['configuration']['variables']['lut']['user']
                    lut.update(time_look_observed='0h', time_look_forecast='1h',
                               time_horizon_observed=None, time_horizon_forecast=None,
                               time_priority_observed='look', time_priority_forecast='look',
                               ensemble_end=2)
                    settings = root / (profile + '.json')
                    settings.write_text(json.dumps(config))
                    result = subprocess.run(
                        [sys.executable, '-B', '-m',
                         'workflow.runner.datasets.app_runner_workflow_hmc_datasets_main',
                         '-settings_file', str(settings), '-time', '2026-11-09 09:35',
                         '--dry-run', '--plan-files'],
                        cwd=repository, env=environment, capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr + result.stdout[-2000:])
                    summary = json.loads(result.stdout[result.stdout.rfind('\n{\n') + 1:])
                    self.assertEqual(len(summary['time_period']), 2)
                    self.assertEqual(summary['source_files'], sources)
                    self.assertEqual(summary['source_files_available'], 0)
                    self.assertEqual(summary['destination_files'], outputs)
                    rows = summary['file_plan']
                    self.assertEqual(len(rows), outputs * 5)
                    checked = [row for row in rows if -3 <= row['time_step'] <= 1]
                    unchecked = [row for row in rows if not -3 <= row['time_step'] <= 1]
                    self.assertEqual(summary['source_files_missing'], len({row['file_source'] for row in checked}))
                    self.assertEqual(summary['destination_files_missing'], len({row['file_destination'] for row in checked}))
                    self.assertTrue(all(row['source_exists'] is None for row in unchecked))
                    self.assertTrue(all(row['destination_exists'] is None for row in unchecked))
                    self.assertEqual([row['step'] for row in rows], sorted(row['step'] for row in rows))
                    self.assertTrue(all(not row['source_exists'] for row in rows))
                    self.assertTrue(all(not row['destination_exists'] for row in rows))
                    self.assertFalse(any('%' in row['file_destination'] for row in rows))
                    self.assertEqual(summary['configured_workflow_steps'], outputs)
                    self.assertEqual(summary['configured_members'],
                                     ['001', '002'] if profile == 'probabilistic' else ['deterministic'])
                    self.assertFalse((root / 'forcing').exists())


if __name__ == '__main__':
    unittest.main()
