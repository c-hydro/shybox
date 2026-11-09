"""Monitoring HTTP routes use declared applications and actual status records."""
import json
from pathlib import Path
import tempfile
import unittest

from hydrapp.app import create_app
from shybox.monitoring_toolkit.execution import RunMonitor


class MonitoringDashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        config = json.loads(Path('hydrapp/config.json').read_text())
        config['publications']['enabled'] = False
        config['monitoring'] = {'folder': 'status', 'stale_seconds': 180, 'applications': {
            'hmc': {'name': 'Continuum', 'run_name': 'icon_marche', 'application': 'hmc'},
            'converter': {'run_name': 'icon_marche', 'application': 'converter'}}}
        self.path = self.root / 'config.json'
        self.path.write_text(json.dumps(config))
        self.client = create_app(self.path).test_client()

    def test_page_and_navigation(self):
        response = self.client.get('/monitoring')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Execution attempts', response.data)
        self.assertIn(b'/monitoring', self.client.get('/').data)
        self.assertEqual(self.client.get('/static/monitoring.js').status_code, 200)

    def test_relative_folder_and_declared_applications(self):
        with RunMonitor(self.root / 'status', run_name='icon_marche', application='hmc',
                        run_time='2026-11-06T12:00:00Z'):
            pass
        with RunMonitor(self.root / 'status', run_name='unlisted', application='private'):
            pass
        data = self.client.get('/api/monitoring').json
        self.assertEqual(data['counts'], {'succeeded': 1, 'missing': 1})
        self.assertEqual(len(data['executions']), 1)
        self.assertEqual(data['executions'][0]['application_id'], 'hmc')
        data = self.client.get('/api/monitoring?application=converter').json
        self.assertEqual(len(data['applications']), 1)
        self.assertEqual(data['executions'], [])

    def test_invalid_filters_and_empty_day(self):
        self.assertEqual(self.client.get('/api/monitoring?date=invalid').status_code, 422)
        self.assertEqual(self.client.get('/api/monitoring?application=unknown').status_code, 404)
        data = self.client.get('/api/monitoring?date=2026-11-07').json
        self.assertEqual(data['counts'], {'missing': 2})

    def test_old_dashboard_config_remains_supported(self):
        config = json.loads(self.path.read_text()); config.pop('monitoring')
        self.path.write_text(json.dumps(config))
        client = create_app(self.path).test_client()
        self.assertEqual(client.get('/api/monitoring').json['applications'], [])
        self.assertEqual(client.get('/').status_code, 200)


if __name__ == '__main__':
    unittest.main()
