"""Lazy discovery across built-in toolkits and custom process locations."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from shybox.orchestrator_toolkit.lib_orchestrator_utils_discovery import (
    find_process_module, load_process,
)
from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES


class TestProcessDiscovery(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(patch.dict(PROCESSES))
        self.modules_before = set(sys.modules)

    def tearDown(self):
        for name in set(sys.modules) - self.modules_before:
            if name.startswith(('_shybox_processes_', 'discovery_test_package')):
                sys.modules.pop(name, None)

    def source(self, name, text):
        path = self.root / name
        path.write_text(text)
        return path

    def test_default_locations_cover_multiple_toolkits_without_importing_them(self):
        with patch('shybox.orchestrator_toolkit.lib_orchestrator_utils_discovery.import_module') as loader:
            self.assertEqual(find_process_module('mask_data_by_ref'),
                             'shybox.processing_grid_toolkit.lib_proc_mask')
            self.assertEqual(find_process_module('exec_rfarm_nwp'),
                             'shybox.processing_rfarm_toolkit.rfarm_adapter')
        loader.assert_not_called()

    def test_custom_directory_loads_only_requested_module_and_supports_relative_imports(self):
        path = self.source('methods.py', '''from .constants import VALUE
from shybox import as_process
@as_process()
def discovery_custom_process(data):
    return data + VALUE
''')
        self.source('constants.py', 'VALUE = 2\n')
        self.source('unrelated.py', 'raise RuntimeError("must not import unrelated modules")\n')
        self.assertEqual(find_process_module('discovery_custom_process', self.root), path)
        process = load_process('discovery_custom_process', self.root)
        self.assertIs(process, PROCESSES['discovery_custom_process'])
        self.assertIs(load_process('discovery_custom_process', '/missing/path'), process)

    def test_additional_dotted_package_and_module(self):
        package = self.root / 'discovery_test_package'
        package.mkdir()
        (package / '__init__.py').write_text('')
        source = package / 'methods.py'
        source.write_text('def dotted_custom_process(data):\n    return data\n')
        with patch.object(sys, 'path', [str(self.root), *sys.path]):
            for location in ('discovery_test_package', 'discovery_test_package.methods'):
                self.assertEqual(find_process_module('dotted_custom_process', location),
                                 'discovery_test_package.methods')

    def test_missing_and_ambiguous_names_are_reported(self):
        with self.assertRaisesRegex(ValueError, 'was not found'):
            find_process_module('discovery_missing_process')
        self.source('duplicate.py', 'def mask_data_by_ref(data):\n    return data\n')
        with self.assertRaisesRegex(ValueError, 'Ambiguous process'):
            find_process_module('mask_data_by_ref', [self.root])

    def test_function_without_process_decorator_does_not_register(self):
        path = self.source('plain.py', 'def discovery_plain_function(data):\n    return data\n')
        with self.assertRaisesRegex(ValueError, '@as_process'):
            load_process('discovery_plain_function', path)

    def test_missing_dependency_in_selected_module_is_not_hidden(self):
        path = self.source('broken.py', '''import discovery_nonexistent_dependency
def discovery_broken_process(data):
    return data
''')
        with self.assertRaisesRegex(ModuleNotFoundError, 'discovery_nonexistent_dependency'):
            load_process('discovery_broken_process', path)

    def test_invalid_names_and_locations_are_reported(self):
        for name in (None, '', 'invalid.method'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                find_process_module(name)
        with self.assertRaisesRegex(ValueError, 'Cannot find process location'):
            find_process_module('discovery_missing_process', ['discovery_missing_package'])


if __name__ == '__main__':
    unittest.main()
