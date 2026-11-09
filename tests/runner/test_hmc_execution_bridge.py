"""HMC orchestration checks actual analyzer status instead of accepting returns."""
import unittest
from unittest.mock import Mock, patch
from shybox.runner_toolkit.orchestration.lib_utils_orchestration_hmc import execute_hmc_member


class HMCExecutionBridgeTests(unittest.TestCase):
    def execute(self, code, ok, *, dry_run=False, configured_dry_run=None):
        config={'app_namelist':{'description':{'type':'hmc','version':'3.3.0'},'fields':{},'file':{'location':'/tmp/test.namelist'}},
                'app_execution':{'description':{'execution_mode':'deterministic','execution_name':'hmc'}},
                'app_info':{'path':'/tmp','file_name':'test.json','format':'json'},'app_log':Mock()}
        if configured_dry_run is not None:config['app_execution']['description']['dry_run']=configured_dry_run
        analyzer=Mock(ok=ok,exit_code=code,errors=[])
        with patch('shybox.runner_toolkit.orchestration.lib_utils_orchestration_hmc.NamelistTemplateManager'), \
             patch('shybox.runner_toolkit.orchestration.lib_utils_orchestration_hmc.NamelistStructureManager'), \
             patch('shybox.runner_toolkit.orchestration.lib_utils_orchestration_hmc.ExecutionManager') as manager:
            manager.return_value.analyze.return_value=analyzer
            result=execute_hmc_member({'run_id':'001'},config,time_tags={'time_run':'2026-11-07'},dry_run=dry_run,view_table=False)
            self.assertEqual(manager.return_value.run.call_args.kwargs['dry_run'],result['dry_run'])
            return result

    def test_successful_process_is_accepted(self):self.assertEqual(self.execute(0,True)['run_id'],'001')
    def test_failure_stderr_or_unknown_exit_is_rejected(self):
        for code,ok in [(7,False),(0,False),(None,True)]:
            with self.subTest(code=code,ok=ok),self.assertRaises(RuntimeError):self.execute(code,ok)
    def test_dry_run_needs_no_exit_code(self):self.assertTrue(self.execute(None,True,dry_run=True)['dry_run'])
    def test_configured_dry_run_override_is_respected(self):self.assertTrue(self.execute(None,True,configured_dry_run=True)['dry_run'])


if __name__=='__main__':unittest.main()
