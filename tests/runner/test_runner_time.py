import unittest

from shybox.generic_toolkit.lib_utils_test import configure_home
from shybox.generic_toolkit.lib_default_args import collector_data
from shybox.runner_toolkit.time.driver_app_time import DrvTime

class TestTime(unittest.TestCase):

    """
    Tests for runner time class.
    """

    def setUp(self):
        """
        Setup test.
        """
        self.collector_vars = {
            'time_run': '202501091400',
            'path_log': '$HOME/log', 'file_log': 'log.txt',
            'path_namelist': '$HOME/namelist', 'file_namelist': 'namelist.txt',
            'path_tmp': '$HOME/tmp/', 'file_tmp': 'tmp.txt'
        }

        self.time_vars = {
            'time_period': 12, 'time_frequency': 'h', 'time_rounding': 'h',
        }

        self.time_run = '201001091400'

        self.expected_vars = {
            "path_log": "$HOME/log", "file_log": "log.txt",
            "path_namelist": "$HOME/namelist", "file_namelist": "namelist.txt",
            "path_tmp": "$HOME/tmp/", "file_tmp": "tmp.txt",
            "time_run": "201001091400", "time_restart": "201001091300",
            "time_period": 12, "time_frequency": 3600, "time_rounding": "h",
            "time_start": "201001091400", "time_end": "201001100100"
        }


    def test_time_class(self):

        # class to initialize the class time
        driver_time = DrvTime(time_obj=self.time_vars, time_collectors=self.collector_vars)

        # method to configure time variables
        alg_variables_time = driver_time.configure_variable_time(time_run_cmd=self.time_run)

        # method to organize time variables
        alg_variables_time = driver_time.organize_variable_time(
            time_obj=alg_variables_time, collector_obj=self.collector_vars)

        # method to view time variables
        driver_time.view_variable_time(data=alg_variables_time, mode=True)

        # collector data
        collector_data.view()

        # assert variables
        alg_variables_time = configure_home(alg_variables_time)
        alg_variables_expected = configure_home(self.expected_vars)
        assert alg_variables_time == alg_variables_expected


if __name__ == '__main__':
    unittest.main()
