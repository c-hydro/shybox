"""Resolve optional sections under explicit roots within a configuration."""
import os
import tempfile
import unittest
from unittest.mock import patch

from shybox.config_toolkit.config_handler import ConfigManager


class TestNestedSections(unittest.TestCase):
    def test_section_defaults_fill_missing_log_without_overwriting_configured_values(self):
        defaults = ConfigManager.fill_section_by_default(None, fill_method='method-by_tmp')
        self.assertEqual(defaults['path'], os.path.join(tempfile.gettempdir(), 'shybox', 'logs'))
        self.assertEqual(defaults['file_name'], 'shybox.log')

        section = {'path': '/configured/logs', 'file_name': '', 'handlers': []}
        result = ConfigManager.fill_section_by_default(
            section, {'file_name': 'custom.log', 'handlers': ['stream']}, fill_method='method-by_tmp',
        )
        self.assertEqual(result, {'path': '/configured/logs', 'file_name': 'custom.log', 'handlers': []})
        self.assertEqual(section['file_name'], '')
        converted = ConfigManager.fill_section_by_default({'path': float('nan')}, fill_method='method-by_tmp')
        self.assertEqual(converted, defaults)

    def manager(self, top_level=False, **kwargs):
        payload = {'configuration': {'priority': {'reference': 'environment', 'other': 'user'},
                                    'flags': {}, 'variables': {
                                        'lut': {'domain': 'marche', 'alias': '{alias_missing}',
                                                'time_run': '%Y%m%d'},
                                        'format': {}, 'template': {},
                                    },
                                    'application': {'time': {'time_frequency': '1h'}, 'output': {
                                        'path': 'runs/{domain}/{time_run}',
                                        'files': ['{domain}.nc', {'fallback': '{missing}', 'alias': '{alias}'}],
                                    }}}}
        if top_level:
            payload['application'] = {'time': {'time_frequency': '30min'}}
        return ConfigManager.from_source(payload, root_key='configuration',
                                         auto_merge_lut=False, auto_env_override=False, auto_validate=False,
                                         auto_fill_lut=False, convert_none_to_nan=False, **kwargs)

    def test_application_root_is_relative_to_configuration(self):
        manager = self.manager()
        result = manager.get_section(root_key='application', section='time', raise_if_missing=True)
        self.assertEqual(result, {'time_frequency': '1h'})
        result['time_frequency'] = '2h'
        self.assertEqual(manager.get_section(root_key='application', section='time')['time_frequency'], '1h')

    def test_top_level_root_retains_priority(self):
        self.assertEqual(self.manager(top_level=True).get_section(root_key='application', section='time'),
                         {'time_frequency': '30min'})

    def test_missing_section_behavior(self):
        manager = self.manager()
        self.assertIsNone(manager.get_section(root_key='application', section='missing'))
        with self.assertRaises(KeyError):
            manager.get_section(root_key='application', section='missing', raise_if_missing=True)

    def test_get_section_expands_nested_values_and_warns_about_unresolved_tags(self):
        manager = self.manager()
        with patch.object(manager.logger, 'warning') as warning:
            result = manager.get_section('output', root_key='application', update_by_lut=True)

        self.assertEqual(result, {
            'path': 'runs/marche/{time_run}',
            'files': ['marche.nc', {'fallback': '{missing}', 'alias': '{alias_missing}'}],
        })
        warning.assert_called_once_with('Settings unresolved placeholder(s): alias_missing missing time_run')
        raw = manager.get_section('output', root_key='application', update_by_lut=False)
        self.assertEqual(raw['path'], 'runs/{domain}/{time_run}')

    def test_get_mandatory_section_fills_from_flat_lut_without_warning(self):
        manager = self.manager(flat_variables=True)
        with patch.object(manager.logger, 'warning') as warning:
            result = manager.get_section('application', update_by_lut=True, warning_not_filled_by_lut=False)

        self.assertEqual(result['output']['path'], 'runs/marche/{time_run}')
        self.assertEqual(result['output']['files'][1]['fallback'], '{missing}')
        warning.assert_not_called()

    def test_get_section_expands_home_paths_introduced_by_lut(self):
        manager = self.manager()
        manager.variables['lut'].update({'data_dir': '$HOME/data', 'cache_dir': '~/.cache'})
        manager.application['paths'] = {'data': '{data_dir}', 'cache': ['{cache_dir}']}

        with patch.dict(os.environ, {'HOME': '/test/home'}):
            optional = manager.get_section('paths', root_key='application', update_by_lut=True)
            mandatory = manager.get_section('application', update_by_lut=True, warning_not_filled_by_lut=False)

        expected = {'data': '/test/home/data', 'cache': ['/test/home/.cache']}
        self.assertEqual(optional, expected)
        self.assertEqual(mandatory['paths'], expected)
        self.assertEqual(manager.variables['lut']['data_dir'], '$HOME/data')


if __name__ == '__main__':
    unittest.main()
