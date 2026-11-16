"""S3M rejects ensemble sources before availability or workflow execution."""
from unittest.mock import Mock, patch
import unittest

from workflow.runner.datasets import app_runner_workflow_s3m_datasets_main as runner


class TestDatasetWorkflow(unittest.TestCase):
    def test_s3m_rejects_probabilistic_plan_before_execution(self):
        application = {
            'info': {'model': 's3m'}, 'time': {},
            'planning': {'options': {}},
            'log': {'path': '/tmp', 'file_name': 'unused.txt'},
            'geo': {'terrain': {'path': '/tmp', 'file_name': 'unused.dem.txt'}},
            'data_source': {'observed': {}, 'forecast': {}}, 'data_destination': {},
        }
        manager = Mock()
        sections = {'application': application, 'workflow': {}, 'lut': {}, 'format': {}, 'template': {}}
        manager.get_section.side_effect = lambda section, **kwargs: sections[section]
        manager.fill_obj_from_lut.side_effect = lambda section, **kwargs: section
        planner = Mock(mode='probabilistic')
        with patch.object(runner.ArgumentsManager, 'get', return_value=('unused.json', None)), \
             patch.object(runner.ConfigManager, 'from_source', return_value=manager), \
             patch.object(runner.LoggingManager, 'setup'), \
             patch.object(runner, 'DataLocal'), \
             patch.object(runner.DatasetPlanner, 'from_config', return_value=planner), \
             patch.object(runner.Orchestrator, 'configure') as configure:
            with self.assertRaisesRegex(ValueError, 'deterministic observed/forecast sources only'):
                runner.main()
        planner.update_time.assert_not_called()
        configure.assert_not_called()
