"""Deterministic/ensemble scheduling, retries and isolation of member settings."""
from collections import Counter
import threading
import unittest

from shybox.runner_toolkit.orchestration.orchestration_handler import OrchestrationManager


class OrchestrationTests(unittest.TestCase):
    def options(self, **kwargs):
        return {'simulation_mode':'probabilistic','members':{'start':1,'end':3},**kwargs}

    def test_deterministic_callback_and_summary(self):
        manager=OrchestrationManager({},lambda spec,cfg:cfg['value'])
        result=manager.run({'value':7})
        self.assertEqual(result['deterministic'].output,7)
        self.assertEqual(manager.summary()['successful'],1)

    def test_member_paths_are_isolated_and_model_tags_preserved(self):
        source={'path':'/run/{ensemble_member}/$yyyy/','items':[{'member':'{ensemble_member_id}'}]}
        def execute(spec,cfg):
            self.assertEqual(cfg['path'],f'/run/{spec.member}/$yyyy/')
            cfg['items'].append({'changed':True})
            return cfg
        manager=OrchestrationManager(self.options(),execute)
        results=manager.run(source)
        self.assertEqual(list(results),['001','002','003'])
        self.assertEqual(len(source['items']),1)
        self.assertTrue(all(len(result.output['items'])==2 for result in results.values()))

    def test_parallel_members_really_overlap(self):
        barrier=threading.Barrier(2)
        def execute(spec,cfg):barrier.wait(timeout=3);return spec.member
        manager=OrchestrationManager(self.options(members={'values':[1,2]},execution_strategy='parallel',max_workers=2),execute)
        self.assertEqual(len(manager.run({})),2)
        self.assertEqual(manager.summary()['failed'],0)

    def test_retry_rebuilds_member_config_after_failure(self):
        calls=Counter()
        def execute(spec,cfg):
            calls[spec.run_id]+=1
            self.assertEqual(cfg['items'],[])
            cfg['items'].append(1)
            if calls[spec.run_id]==1:raise RuntimeError('transient')
            return 'ok'
        manager=OrchestrationManager(self.options(max_retries=1),execute)
        self.assertTrue(all(result.attempts==2 for result in manager.run({'items':[]}).values()))

    def test_continue_on_error_keeps_failed_member_details(self):
        def execute(spec,cfg):
            if spec.member=='002':raise RuntimeError('member input missing')
            return spec.member
        manager=OrchestrationManager(self.options(continue_on_error=True),execute)
        results=manager.run({})
        self.assertFalse(results['002'].success)
        self.assertIn('input missing',results['002'].error)
        self.assertEqual(manager.summary()['successful_runs'],['001','003'])

    def test_fail_fast_stops_later_sequential_members(self):
        calls=[]
        def execute(spec,cfg):calls.append(spec.member);raise ValueError('bad')
        manager=OrchestrationManager(self.options(),execute)
        with self.assertRaises(RuntimeError):manager.run({})
        self.assertEqual(calls,['001'])

    def test_invalid_configuration_and_unsafe_member_ids_fail_before_execution(self):
        bad=[{'simulation_mode':'invalid'},{'max_workers':0},{'max_retries':-1},
             {'continue_on_error':'yes'},self.options(members={'values':[1,1]}),
             self.options(members={'values':['../unsafe']}),self.options(members={'start':4,'end':1})]
        for options in bad:
            with self.subTest(options=options),self.assertRaises(ValueError):OrchestrationManager(options,lambda *_:None)


if __name__=='__main__':unittest.main()
