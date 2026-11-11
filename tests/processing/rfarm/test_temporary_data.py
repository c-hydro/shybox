"""Temporary ensemble storage and RFarm configuration resolution."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import xarray as xr

from shybox.config_toolkit.config_handler import ConfigManager
from shybox.dataset_toolkit.dataset_handler_temporary import DataTemporary


class TestDataTemporary(unittest.TestCase):
    def test_storage_is_lazy_and_runs_are_isolated(self):
        with tempfile.TemporaryDirectory() as root:
            storage = DataTemporary(str(Path(root) / 'members'), 'rain_{ensemble}.pkl')
            self.assertFalse(Path(storage.folder_name).exists())
            first, second = storage.create_run(), storage.create_run()
            self.assertNotEqual(first.folder_name, second.folder_name)
            self.assertEqual(first.path(ensemble='002').name, 'rain_002.pkl')
            self.assertEqual(list(Path(first.folder_name).iterdir()), [])

    def test_single_member_pickle_roundtrip(self):
        with tempfile.TemporaryDirectory() as root:
            storage = DataTemporary(root).create_run()
            original = xr.DataArray([1.0, 2.0], dims='time', name='rain', attrs={'units': 'mm'})
            path = storage.write_data(original)
            self.assertTrue(path.is_file())
            xr.testing.assert_identical(original, storage.read_data())
            with self.assertRaises(FileNotFoundError):
                DataTemporary(root, 'missing.pkl').read_data()

    def test_rejects_unresolved_paths_and_unsafe_templates(self):
        with self.assertRaises(ValueError):
            DataTemporary('{path_tmp}')
        for name in ('../data.pkl', 'data_{step.value}.pkl', 'data_{step[0]}.pkl'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                DataTemporary('/tmp', name)

    def test_json_resolves_forecast_tags_but_preserves_member_placeholder(self):
        settings_path = Path(__file__).resolve().parents[3] / (
            'workflow/dataset/disaggregate/app_rfarm_workflow_hmc_icon.json')
        env = {'DOMAIN_NAME': 'marche', 'PATH_SRC': '/tmp/input', 'PATH_DST': '/tmp/output',
               'TIME_PERIOD': '72', 'TIME_START': '2026-10-31 00:00', 'TIME_END': '2026-10-31 00:00',
               'PATH_LOG': '/tmp/log', 'PATH_TMP': '/tmp/rfarm', 'PATH_GEO': '/tmp/geo'}
        with patch.dict(os.environ, env):
            cfg = ConfigManager.from_source(
                str(settings_path), root_key='configuration', auto_validate=True,
                auto_fill_lut=True, flat_variables=True, flat_key_mode='value')
            application = cfg.fill_obj_from_lut(
                section=cfg.get_section(section='application'), resolve_time_placeholders=False,
                time_keys=('time_start', 'time_end', 'time_period'), template_keys=('file_time_destination',))
            when = pd.Timestamp('2026-10-31 00:00')
            application = cfg.fill_section_with_times(
                application, time_values={'file_time_destination': '', 'path_time_source': when,
                                          'file_time_source': when, 'path_time_destination': when,
                                          'path_time_run': when})
        storage = DataTemporary(
            folder_name=application['data_ancillary']['path'],
            file_name=application['data_ancillary']['file_name'],
            writer_type=application['data_ancillary']['writer_type'],
            prefix=application['data_ancillary']['prefix'],
        )
        self.assertNotIn('data_ancillary', application['data_source'])
        self.assertEqual(application['tmp']['path'], '/tmp/rfarm/')
        self.assertTrue(pd.isna(application['tmp']['file_name']))
        self.assertEqual(Path(storage.folder_name), Path('/tmp/rfarm/marche/2026/10/31/0000'))
        self.assertEqual(storage.file_name, 'rf.icon-2i.20261031_{ensemble}.pkl')
        self.assertEqual(storage.writer_type, 'pickle')
        self.assertEqual(storage.prefix, 'rfarm_')

    def test_generic_templates_and_configurable_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            storage = DataTemporary(root, 'field_{step:03d}_{variable}.pkl', prefix='analysis_')
            run = storage.create_run()
            self.assertTrue(Path(run.folder_name).name.startswith('analysis_'))
            overridden = storage.create_run(file_name='replacement.pkl')
            self.assertEqual(overridden.file_name, 'replacement.pkl')
            self.assertEqual(storage.file_name, 'field_{step:03d}_{variable}.pkl')
            run.write_data({'value': 12}, tags={'step': 2, 'variable': 'temperature'})
            self.assertEqual(run.read_data(tags={'step': 2, 'variable': 'temperature'}), {'value': 12})
            self.assertEqual(run.path(step=2, variable='temperature').name, 'field_002_temperature.pkl')
            with self.assertRaises(ValueError):
                run.path(step=2, variable='../outside')
            with self.assertRaises(ValueError):
                run.path(step=2)

    def test_writer_defaults_and_invalid_options(self):
        for writer, suffix in (('pickle', '.pkl'), ('netcdf', '.nc'), ('zarr', '.zarr')):
            self.assertEqual(DataTemporary('/tmp', writer_type=writer).file_name, 'data' + suffix)
        with self.assertRaises(ValueError):
            DataTemporary('/tmp', writer_type='unsupported')
        with self.assertRaises(ValueError):
            DataTemporary('/tmp', prefix='../outside')

    def test_array_and_dataset_roundtrips(self):
        array = xr.DataArray([1.0, float('nan'), 2.0], dims='x', coords={'x': [1, 2, 3]},
                             name='rain', attrs={'units': 'mm'})
        dataset = xr.Dataset({'rain': array}, attrs={'domain': 'test'})
        for writer in ('netcdf', 'zarr'):
            for original in (array, array.rename(None), dataset):
                with self.subTest(writer=writer, kind=type(original).__name__, name=getattr(original, 'name', None)):
                    with tempfile.TemporaryDirectory() as root:
                        storage = DataTemporary(root, writer_type=writer)
                        original_copy = original.copy(deep=True)
                        storage.write_data(original)
                        with storage.read_data() as restored:
                            self.assertIsInstance(restored, type(original))
                            xr.testing.assert_identical(restored, original)
                        xr.testing.assert_identical(original_copy, original)
                        xr.testing.assert_identical(storage.read_data(load=True), original)

    def test_netcdf_reads_are_lazy_and_writer_options_are_forwarded(self):
        with tempfile.TemporaryDirectory() as root:
            storage = DataTemporary(root, writer_type='netcdf')
            original = xr.DataArray([1.0, 2.0], dims='x', name='rain')
            storage.write_data(original, encoding={'rain': {'zlib': True}})
            with storage.read_data() as restored:
                self.assertFalse(restored.variable._in_memory)
                self.assertTrue(restored.encoding['zlib'])
                self.assertEqual(float(restored.isel(x=0)), 1.0)
            loaded = storage.read_data(load=True)
            self.assertTrue(loaded.variable._in_memory)

    def test_array_backends_reject_arbitrary_python_objects(self):
        for writer in ('netcdf', 'zarr'):
            with tempfile.TemporaryDirectory() as root, self.assertRaises(TypeError):
                DataTemporary(root, writer_type=writer).write_data({'not': 'an xarray object'})

    def test_zarr_preserves_chunks_and_reads_lazily(self):
        with tempfile.TemporaryDirectory() as root:
            storage = DataTemporary(root, writer_type='zarr')
            original = xr.DataArray([1.0, 2.0, 3.0, 4.0], dims='x', name='rain').chunk({'x': 2})
            storage.write_data(original)
            with storage.read_data() as restored:
                self.assertEqual(restored.chunks, ((2, 2),))
                self.assertFalse(restored.variable._in_memory)
                self.assertEqual(float(restored.isel(x=0).compute()), 1.0)


if __name__ == '__main__':
    unittest.main()
