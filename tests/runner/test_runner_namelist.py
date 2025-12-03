import unittest
import os

from shybox.generic_toolkit.lib_utils_test import configure_home, extract_obj_by_list
from shybox.default.lib_default_args import collector_data
from shybox.runner_toolkit.namelist.old.driver_app_namelist import DrvNamelist

class TestNamelist(unittest.TestCase):

    """
    Tests for runner namelist class.
    """

    def setUp(self):
        """
        Setup test.
        """

        self.collector_vars = {
            'time_run': '202205231600',
            'time_period': 24, 'time_frequency': 3600, 'time_rounding': 'h', 'time_shift': 1,
            'time_restart': '202205231600',
            'time_start': '202205231600', 'time_end': '202205241500',
            'domain_name': 'marche',
            'file_namelist': 'namelist.txt', 'path_namelist': '$HOME/namelist',
            'file_log': 'log.txt', 'path_log': '$HOME/log',
            'file_tmp': 'tmp.txt', 'path_tmp': '$HOME/tmp/',
            'path_source': '$HOME/run_path_source/', 'path_destination': '$HOME/run_path_destination/'
        }

        self.namelist_version, self.namelist_type = '3.1.6', 'hmc'
        self.namelist_vars = {
            "fields": {
                "by_value": {
                    "sDomainName": "{domain_name}",
                    "sPathData_Static_Gridded": "$HOME/run_path_geo/{domain_name}/gridded/",
                    "sPathData_Static_Point": "$HOME/run_path_geo/{domain_name}/point/",
                    "sPathData_Forcing_Gridded": "{path_source}/{domain_name}/gridded/",
                    "sPathData_Forcing_Point": "{path_source}/{domain_name}/point/",
                    "sPathData_Forcing_TimeSeries": "{path_destination}/{domain_name}/time_series/",
                    "sPathData_Updating_Gridded": "{path_source}/{domain_name}/gridded/",
                    "sPathData_Output_Gridded": "{path_destination}/{domain_name}/gridded/$yyyy/$mm/$dd/",
                    "sPathData_Output_Point": "{path_destination}/{domain_name}/point/$yyyy/$mm/$dd/",
                    "sPathData_Output_TimeSeries": "{path_destination}/{domain_name}/point/",
                    "sPathData_State_Gridded": "{path_destination}/{domain_name}/gridded/$yyyy/$mm/$dd/",
                    "sPathData_State_Point": "{path_destination}/{domain_name}/point/$yyyy/$mm/$dd/",
                    "sPathData_Restart_Gridded": "{path_destination}/{domain_name}/gridded/$yyyy/$mm/$dd/",
                    "sPathData_Restart_Point": "{path_destination}/{domain_name}/point/$yyyy/$mm/$dd/",
                    "sTimeStart": "{time_run}",
                    "sTimeRestart": "{time_restart}",
                    "iSimLength": "{time_period}",
                    "iTVeg": 800
                },
                "by_pattern": {
                    "pattern_dt": {"active": True, "template": "iDt", "value": 3600},
                }
            }
        }

        self.expected_vars = {
            "time_run": "202205231600", "time_restart": "202205231600",
            "time_period": 24, "time_frequency": 3600, "time_rounding": "h", "time_shift": 1,
            "time_start": "202205231600", "time_end": "202205241500",
            "domain_name": "marche",
            "file_namelist": "namelist.txt", "path_namelist": "$HOME/namelist",
            "file_log": "log.txt", "path_log": "$HOME/log",
            "file_tmp": "tmp.txt", "path_tmp": "$HOME/tmp/",
            "path_source": "$HOME/run_path_source/",
            "path_destination": "$HOME/run_path_destination/"
        }


    def test_namelist_class(self):

        # configure home
        collector_vars = configure_home(self.collector_vars)
        # configure time
        time_vars = extract_obj_by_list(collector_vars,
                            ['time_run', 'time_restart', 'time_period', 'time_frequency', 'time_shift', 'time_rounding'])

        # driver namelist variable(s)
        driver_namelist = DrvNamelist(
            namelist_obj=self.namelist_vars, time_obj=time_vars,
            namelist_version=self.namelist_version, namelist_type=self.namelist_type,
            namelist_update=True)

        # method to define namelist file(s)
        alg_namelist_file = driver_namelist.define_file_namelist(
            settings_variables=collector_vars,
            app_path_def=os.path.dirname(os.path.realpath(__file__)), app_path_force=True)
        # method to get namelist structure
        alg_namelist_default = driver_namelist.get_structure_namelist()
        # method to define namelist variable(s)
        alg_namelist_by_value = driver_namelist.define_variable_namelist(settings_variables=collector_vars)
        # method to combine namelist variable(s)
        alg_namelist_defined, alg_namelist_checked, alg_namelist_collections = driver_namelist.combine_variable_namelist(
            variables_namelist_default=alg_namelist_default, variables_namelist_by_value=alg_namelist_by_value)
        # method to dump namelist structure
        driver_namelist.dump_structure_namelist(alg_namelist_defined)
        # method to organize variable namelist
        namelist_vars = driver_namelist.organize_variable_namelist(namelist_obj=collector_vars)

        # method to view namelist variable(s)
        driver_namelist.view_variable_namelist(data=alg_namelist_defined, mode=True)

        # collector data
        collector_data.view(table_print=True)

        # assert variables
        alg_variables_expected = configure_home(self.expected_vars)
        assert namelist_vars == alg_variables_expected

if __name__ == '__main__':
    unittest.main()