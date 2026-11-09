"""Full Ruler reporting pipeline, package isolation and backwards imports."""
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from shybox.monitoring_toolkit.common import read_status
from shybox.monitoring_toolkit.execution import RunMonitor
from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor
from shybox.monitoring_toolkit.resources.ruler import RulerMonitor, load_ruler_config, workspace_to_csv


class RulerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.config=load_ruler_config('workflow/monitoring/ruler_native.example.json')
        for key in ('ancillary','destination','figure'):
            self.config['report'][key]['folder_name']=str(self.root)
        # Use the current process name for native Ruler's name/command selector.
        import psutil
        self.config['process']['name']=psutil.Process(os.getpid()).name()

    def test_complete_native_workspace_csv_and_plot_pipeline(self):
        monitor=RulerMonitor(self.config)
        # Freeze two real observations without sleeping through the timed loop.
        import pandas as pd
        first=monitor.sample()
        self.assertIn('virtual_memory_total',first)
        self.assertIn('disk_usage_total',first)
        self.assertIn('system_load_1m',first)
        self.assertTrue(any(key.endswith('_pid') for key in first))
        monitor.driver.freeze_report_analysis(pd.Timestamp('2026-11-07T00:00:00Z'), first)
        monitor.driver.freeze_report_analysis(pd.Timestamp('2026-11-07T00:00:02Z'), monitor.sample())
        monitor.dump().view()
        csv=Path(monitor.driver.report_file_path_destination)
        self.assertTrue(csv.is_file())
        data=pd.read_csv(csv,sep=';')
        self.assertEqual(len(data),2)
        images=list(self.root.glob('*.png'));self.assertEqual(len(images),3)
        self.assertTrue(all(path.read_bytes().startswith(b'\x89PNG') for path in images))
        workspaces=list(self.root.glob('*.workspace'));self.assertTrue(workspaces)
        merged=workspace_to_csv(workspaces,self.root/'merged.csv')
        self.assertEqual(len(pd.read_csv(merged,sep=';')),2)

    def test_process_filter_and_sort_native_behaviour(self):
        from shybox.monitoring_toolkit.resources.ruler.engine.lib_analysis_fx import filter_process_info,sort_process_info
        self.assertEqual(filter_process_info(['a','b'],'first'),['a'])
        self.assertEqual(filter_process_info(['a','b'],'last'),['b'])
        self.assertEqual(filter_process_info(['a','b'],'all'),['a','b'])
        self.assertEqual(sort_process_info(['a','b'],[2,1],'ascending'),['b','a'])

    def test_native_report_never_invokes_process_control(self):
        flags=self.config['algorithm']['flags']['algorithm']
        flags.update(activate_report_organize=False,activate_report_dump=False,activate_report_view=False)
        with patch('shybox.monitoring_toolkit.resources.ruler.engine.driver_analysis.DriverAnalysis.kill') as kill:
            RulerMonitor(self.config).run()
        kill.assert_not_called()
        flags['activate_report_kill']=True
        with self.assertRaisesRegex(ValueError,'Process control'):RulerMonitor(self.config)

    def test_time_limits_and_native_config_paths(self):
        self.assertTrue(Path(self.config['report']['destination']['folder_name']).is_absolute())
        from shybox.monitoring_toolkit.resources.ruler.engine.lib_utils_time import check_time_delta_limits
        self.assertEqual(check_time_delta_limits('1s',time_delta_min='2s'),'2s')
        self.assertEqual(check_time_delta_limits('2h',time_delta_max='1h'),'1h')
        self.assertEqual(check_time_delta_limits('2d',time_delta_max='1h'),'1h')

    def test_previous_public_paths_still_resolve_to_same_implementations(self):
        from shybox.monitoring_toolkit import RunMonitor as root_monitor
        from shybox.monitoring_toolkit.memory_monitor import ProcessMemoryMonitor as old_resources
        from shybox.monitoring_toolkit.monitoring_handler import RunMonitor as old_monitor
        self.assertIs(root_monitor,RunMonitor);self.assertIs(old_monitor,RunMonitor)
        self.assertIs(old_resources,ProcessMemoryMonitor)

    def test_engine_modules_are_namespaced_and_import_loop_does_not_run(self):
        importlib.import_module('shybox.monitoring_toolkit.resources.ruler.engine.lib_data_io')
        self.assertNotIn('lib_data_io',sys.modules)
        command=[sys.executable,'-B','-c','import shybox.monitoring_toolkit.resources.ruler.engine.ruler_utils_loop_fx']
        subprocess.run(command,check=True,timeout=5,capture_output=True)

    def test_execution_and_resources_remain_independent(self):
        with RunMonitor(self.root/'execution',run_name='test',application='worker') as monitor:
            pass
        resource=ProcessMemoryMonitor({'processes':{'self':{'pid':os.getpid()}},'interval_seconds':1,
                                      'output':{'folder_name':str(self.root/'resources')}})
        resource.write(resource.sample())
        self.assertEqual(read_status(monitor.file_path)['status'],'succeeded')
        self.assertTrue((self.root/'resources/memory.latest.json').is_file())


if __name__=='__main__':unittest.main()
