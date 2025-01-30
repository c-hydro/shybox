import unittest
import os

from shybox.generic_toolkit.lib_utils_test import configure_home
from shybox.generic_toolkit.lib_default_args import collector_data
from shybox.runner_toolkit.settings.driver_app_settings import DrvSettings

class TestSettings(unittest.TestCase):

    """
    Tests for runner settings class.
    """

    def setUp(self):
        """
        Setup test.
        """
        self.collector_vars = {
            'path_log': '$HOME/log', 'file_log': 'log.txt',
            'path_namelist': '$HOME/namelist', 'file_namelist': 'namelist.txt',
            'path_tmp': '$HOME/tmp/', 'file_tmp': 'tmp.txt'
        }

        self.domain_name = 'test'
        self.time_run = '202501091400'

        self.file_name = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'test_runner_settings.json')

        self.expected_vars = {
            'domain_name': 'test',
            'path_log': '$HOME/log', 'file_log': 'log.txt',
            'path_namelist': '$HOME/namelist', 'file_namelist': 'namelist.txt',
            'path_tmp': '$HOME/tmp/', 'file_tmp': 'tmp.txt',
            'time_run': '202501091400', 'time_restart': None
        }

    def test_settings_class_by_file(self):

        # method to initialize settings class
        driver_settings = DrvSettings(file_name=self.file_name, file_time=self.time_run,
                                      file_key='information', settings_collectors=self.collector_vars)

        # method to configure variable settings
        (alg_variables_settings, alg_variables_collector,
         alg_variables_system) = driver_settings.configure_variable_by_settings()
        # method to organize variable settings
        alg_variables_settings = driver_settings.organize_variable_settings(
            alg_variables_settings, alg_variables_collector)
        # method to view variable settings
        driver_settings.view_variable_settings(data=alg_variables_settings, mode=True)

        # collector data
        collector_data.view()

        # assert variables
        alg_variables_expected = configure_home(self.expected_vars)
        assert alg_variables_settings == alg_variables_expected

    def test_settings_class_by_dict(self):

        # method to initialize settings class
        driver_settings = DrvSettings(file_name=None, file_time=self.time_run,
                                      file_key='information', settings_collectors=self.collector_vars)

        # method to configure variable settings
        (alg_variables_settings, alg_variables_collector,
         alg_variables_system) = driver_settings.configure_variable_by_dict(
            other_settings={'domain_name': self.domain_name, 'time_run': self.time_run, 'time_restart': None})

        # method to organize variable settings
        alg_variables_settings = driver_settings.organize_variable_settings(
            alg_variables_settings, alg_variables_collector)

        # method to view variable settings
        driver_settings.view_variable_settings(data=alg_variables_settings, mode=True)

        # collector data
        collector_data.view()

        # assert variables
        alg_variables_expected = configure_home(self.expected_vars)
        assert alg_variables_settings == alg_variables_expected

if __name__ == '__main__':
    unittest.main()

