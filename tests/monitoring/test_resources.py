"""Actual process RAM sampling, CSV/PNG output and native Ruler collectors."""
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor, load_memory_config, plot_memory_history
from shybox.monitoring_toolkit.resources import read_memory_snapshot


class ResourceMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.config={'interval_seconds':.01,'output':{'folder_name':str(self.root)},
                     'processes':{'self':{'pid':os.getpid(),'include_children':False}}}

    def test_actual_pid_memory_and_thresholds(self):
        self.config['processes']['self'].update(warning_rss_mib=.0001,critical_rss_mib=.001)
        data=ProcessMemoryMonitor(self.config).sample()
        target=data['targets'][0]
        self.assertEqual(target['status'],'running');self.assertGreater(target['rss_mib'],0)
        self.assertEqual(target['level'],'critical');self.assertEqual(target['processes'][0]['pid'],os.getpid())

    def test_child_process_is_included_once(self):
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'])
        self.addCleanup(lambda: child.wait())
        self.addCleanup(lambda: child.kill() if child.poll() is None else None)
        self.config['processes']['self']['include_children']=True
        target=ProcessMemoryMonitor(self.config).sample()['targets'][0]
        ids=[process['pid'] for process in target['processes']]
        self.assertIn(child.pid,ids);self.assertEqual(len(ids),len(set(ids)))

    def test_missing_pid_has_unknown_memory_not_zero(self):
        self.config['processes']={'absent':{'pid':2147483647}}
        target=ProcessMemoryMonitor(self.config).sample()['targets'][0]
        self.assertEqual(target['status'],'missing');self.assertIsNone(target['rss_mib'])
        self.assertEqual(target['level'],'unknown')

    def test_reused_pid_is_not_treated_as_original_process(self):
        monitor=ProcessMemoryMonitor(self.config);monitor.pid_births['self']=-1
        self.assertEqual(monitor.sample()['targets'][0]['status'],'missing')

    def test_csv_snapshot_and_history_plot(self):
        monitor=ProcessMemoryMonitor(self.config)
        self.assertEqual(monitor.run(samples=2),2)
        snapshot=read_memory_snapshot(self.root/'memory.latest.json')
        self.assertEqual(snapshot['status'],'available')
        path=next(self.root.glob('*.csv'))
        with path.open() as stream:self.assertEqual(len(list(csv.DictReader(stream))),2)
        image=self.root/'memory.png';plot_memory_history(path,image)
        self.assertTrue(image.read_bytes().startswith(b'\x89PNG'))

    def test_ruler_native_report_contains_host_and_selected_process(self):
        self.config['ruler']={'enabled':True,'disk_paths':[str(self.root)],'process_memory_type':'partial'}
        report=ProcessMemoryMonitor(self.config).sample()['ruler']
        self.assertEqual(report['source'],'c-hydro/ruler')
        self.assertIn('virtual_memory_total',report['memory'])
        self.assertIn(str(self.root),report['disk'])
        self.assertTrue(report['processes']['self'])
        self.assertTrue(any(key.endswith('_pid') and value==os.getpid() for key,value in report['processes']['self'].items()))

    def test_configuration_paths_and_invalid_selectors(self):
        path=self.root/'config.json'
        for spec in ({'pid':1,'name':'python'},{'pid':0},{'cmdline_contains':''}):
            path.write_text(json.dumps({'memory_monitoring':{'processes':{'bad':spec}}}))
            with self.assertRaises(ValueError):load_memory_config(path)
        path.write_text(json.dumps({'memory_monitoring':{'processes':{'self':{'pid':os.getpid()}},'output':{'folder_name':'records'}}}))
        self.assertEqual(load_memory_config(path)['output']['folder_name'],str(self.root/'records'))


if __name__=='__main__':unittest.main()
