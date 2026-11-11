"""Resolve optional sections under explicit roots within a configuration."""
import unittest

from shybox.config_toolkit.config_handler import ConfigManager


class TestNestedSections(unittest.TestCase):
    def manager(self, top_level=False):
        payload = {'configuration': {'priority': {'reference': 'environment', 'other': 'user'},
                                    'flags': {}, 'variables': {'lut': {}, 'format': {}, 'template': {}},
                                    'application': {'time': {'time_frequency': '1h'}}}}
        if top_level:
            payload['application'] = {'time': {'time_frequency': '30min'}}
        return ConfigManager.from_source(payload, root_key='configuration',
                                         auto_merge_lut=False, auto_env_override=False, auto_validate=False,
                                         auto_fill_lut=False, convert_none_to_nan=False)

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


if __name__ == '__main__':
    unittest.main()
