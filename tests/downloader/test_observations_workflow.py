"""Tests for the remote database to local station CSV workflow."""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from pandas import DataFrame
from psycopg.errors import ConnectionTimeout

from shybox.db_toolkit import lib_db_io
from shybox.db_toolkit.db_handler_base import ConfigurationDB, write_csv_default
from shybox.db_toolkit.access_point_db import access_point_vda
from shybox.db_toolkit.lib_db_providers import DataProvider, get_provider
from workflow.downloader import app_downloader_workflow_obs_ws_main as workflow


class TestSharedDatabaseIO(unittest.TestCase):
    def test_default_csv_writer_accepts_a_full_path(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'nested' / 'rain.csv'
            write_csv_default(target, DataFrame([{'value': 2, 'quality': 0}]), {'delimiter': '|'})
            self.assertEqual(target.read_text().splitlines(), ['value|quality', '2|0'])

    def test_debug_pickle_overwrites_with_the_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'debug' / 'data.pkl'
            lib_db_io.write_pickle({'value': 1}, path)
            self.assertEqual(lib_db_io.write_pickle({'value': 2}, path), path)
            self.assertEqual(lib_db_io.read_pickle(path), {'value': 2})

    def test_writer_can_use_a_provider_default(self):
        writer = Mock()
        data, logger = object(), object()
        destination = {}

        result = lib_db_io.write_data(
            data, destination, writers={'report': writer}, variable='rain', logger=logger, default_format='report',
        )

        self.assertIs(result, writer.return_value)
        writer.assert_called_once_with(data, destination, variable='rain', logger=logger)


class TestRemoteDatabase(unittest.TestCase):
    def test_configuration_keeps_active_variables_in_source_order(self):
        variables = {'rain': {'active': True}, 'snow': {'active': False}, 'temperature': {}}
        output = {'temperature': {}, 'snow': {}, 'rain': {}}
        database = ConfigurationDB({'name': 'observations'}, variables, output=output)

        self.assertEqual(list(database.variables), ['rain', 'temperature'])
        self.assertEqual(list(database.outputs), ['rain', 'temperature'])
        self.assertIs(database.variables, database.config_in)
        self.assertIs(database.outputs, database.config_out)
        self.assertEqual(list(output), ['temperature', 'snow', 'rain'])

    def test_configuration_requires_at_least_one_active_variable(self):
        for variables in ({}, {'rain': {'active': False}}):
            with self.subTest(variables=variables), self.assertRaisesRegex(ValueError, 'at least one active variable'):
                ConfigurationDB({'name': 'observations'}, variables)

    def test_configuration_requires_boolean_active_flags(self):
        with self.assertRaisesRegex(ValueError, r'source\.variables\.rain\.active must be true or false'):
            ConfigurationDB({'name': 'observations'}, {'rain': {'active': 'false'}})

    def test_configuration_requires_matching_active_sources_and_declared_destinations(self):
        variables = {'rain': {'active': True}, 'snow': {'active': False}}
        with self.assertRaisesRegex(ValueError, r'missing from destination: rain.*missing from source.variables: humidity'):
            ConfigurationDB({'name': 'observations'}, variables, output={'humidity': {}})
        database = ConfigurationDB({'name': 'observations'}, variables, output={'rain': {}})
        self.assertEqual(list(database.config_in), ['rain'])
        self.assertEqual(list(database.config_out), ['rain'])
        with self.assertRaisesRegex(KeyError, "No active destination is configured for variable 'snow'"):
            database.write_data(object(), {}, variable='snow')

    def test_backend_selection(self):
        self.assertEqual(ConfigurationDB({'name': 'observations'}).backend, 'postgresql')
        self.assertEqual(ConfigurationDB({'backend': 'PostgreSQL'}).backend, 'postgresql')
        with self.assertRaisesRegex(ValueError, 'Unsupported database backend'):
            ConfigurationDB({'backend': 'unsupported'})

    def test_configuration_rejects_invalid_provider_before_connecting(self):
        invalid_providers = {
            'missing': SimpleNamespace(),
            'noncallable': SimpleNamespace(get_data='value'),
            'incompatible signature': SimpleNamespace(get_data=lambda source, selection: None),
        }
        for reason, provider in invalid_providers.items():
            with (
                self.subTest(reason=reason),
                patch('shybox.db_toolkit.lib_db_providers.import_module', return_value=provider),
                patch('psycopg.connect') as connect,
            ):
                with self.assertRaisesRegex(TypeError, r'must expose get_data\(source, selection, start, end\)'):
                    ConfigurationDB({'name': 'observations'}, provider='vda')
                connect.assert_not_called()

    def test_configuration_rejects_invalid_writer_before_connecting(self):
        invalid_writers = {
            'null': {'write_data': None},
            'noncallable': {'write_data': 'value'},
            'incompatible signature': {'write_data': lambda data, destination: None},
        }
        for reason, methods in invalid_writers.items():
            provider = SimpleNamespace(get_data=access_point_vda.get_data, **methods)
            with (
                self.subTest(reason=reason),
                patch('shybox.db_toolkit.lib_db_providers.import_module', return_value=provider),
                patch('psycopg.connect') as connect,
            ):
                with self.assertRaisesRegex(TypeError, r'must expose write_data\(data, destination'):
                    ConfigurationDB({'name': 'observations'}, provider='vda')
                connect.assert_not_called()

    def test_configured_module_without_writer_uses_shared_csv(self):
        module = ModuleType('external_observations.station_provider')
        module.get_data = Mock(return_value=DataFrame([{'value': 2, 'external_id': 'A', 'quality': 0}]))
        selection = {'variable': 'rain'}
        with patch.dict(sys.modules, {module.__name__: module}):
            database = ConfigurationDB({'name': 'observations'}, {'rain': selection}, provider=module.__name__)

        start = datetime(2026, 11, 25, 11)
        data = database.get_data('rain', start, start)
        module.get_data.assert_called_once_with(database, selection, start, start)
        with tempfile.TemporaryDirectory() as directory:
            database.write_data(data, {'path': directory, 'file_name': 'rain.csv'}, variable='rain')
            self.assertEqual((Path(directory) / 'rain.csv').read_text().splitlines(), [
                'value;external_id;quality',
                '2;A;0',
            ])

    def test_configured_provider_class_can_override_writer(self):
        writer = Mock()

        class Provider(DataProvider):
            def get_data(self, source, selection, start, end):
                return selection

            def write_data(self, data, destination, *, variable, logger):
                return writer(data, destination, variable=variable, logger=logger)

        module = ModuleType('external_observations.class_provider')
        module.Provider = Provider
        module.MissingReader = type('MissingReader', (DataProvider,), {})
        with patch.dict(sys.modules, {module.__name__: module}):
            database = ConfigurationDB({'name': 'observations'}, provider=f'{module.__name__}:Provider')
            with self.assertRaisesRegex(TypeError, 'abstract method.*get_data'):
                get_provider(f'{module.__name__}:MissingReader')

        data, destination = object(), {'file_format': 'custom'}
        self.assertIs(database.write_data(data, destination, variable='rain'), writer.return_value)
        writer.assert_called_once_with(data, destination, variable='rain', logger=database.logger)

    def test_write_data_defaults_to_csv_and_rejects_unknown_formats(self):
        database = ConfigurationDB({'name': 'observations'})
        data = object()
        with patch.object(access_point_vda, 'write_csv_vda') as writer:
            self.assertIs(database.write_data(data, {}), writer.return_value)
            writer.assert_called_once_with(data, {}, variable='observations', logger=database.logger)

        with self.assertRaisesRegex(ValueError, r"Unsupported observation output format 'unknown'.*csv"):
            database.write_data(data, {'file_format': 'unknown'})

    def test_vda_csv_dispatch_uses_the_provider_hook(self):
        database = ConfigurationDB({'name': 'observations'})
        data = object()
        destination = {'format': 'csv', 'path': 'output', 'file_name': 'rain.csv', 'options': {'delimiter': '|'}}
        with patch.object(access_point_vda, 'write_csv_default') as shared_writer:
            self.assertIs(database.write_data(data, destination, variable='rain'), shared_writer.return_value)
        shared_writer.assert_called_once_with(
            Path('output/rain.csv'), data, options=destination['options'], variable='rain', logger=database.logger,
            column={
                'station_name': 'station', 'longitude': 'longitude', 'latitude': 'latitude',
                'post_validity_code': 'code', 'value': 'value', 'time': 'time',
            },
        )

    def test_write_data_dispatches_to_a_format_specific_writer(self):
        data, logger = object(), object()
        destination = {'file_format': ' Report '}
        writer = Mock()
        result = lib_db_io.write_data(data, destination, writers={'report': writer}, variable='rain', logger=logger)
        self.assertIs(result, writer.return_value)
        writer.assert_called_once_with(data, destination, variable='rain', logger=logger)

    def test_auto_prefers_netrc_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            credentials = Path(directory) / 'netrc'
            credentials.write_text('machine example.test login file_user password file_password\n')
            credentials.chmod(0o600)
            with patch.dict(os.environ, {'SHYBOX_DB_USER': 'env_user', 'SHYBOX_DB_PASSWORD': 'env_password'}):
                database = {'host': 'example.test', 'netrc_file': str(credentials)}
                self.assertEqual(ConfigurationDB(database).get_credentials(), ('file_user', 'file_password'))
                database['host'] = 'other.example.test'
                self.assertEqual(ConfigurationDB(database).get_credentials(), ('env_user', 'env_password'))

    def test_connect_uses_credentials_from_handler(self):
        database = ConfigurationDB({'name': 'observations', 'host': '127.0.0.1', 'port': 16432})
        with (
            patch.object(database, 'get_credentials', return_value=('reader', 'test-password')) as credentials,
            patch('psycopg.connect') as connect,
        ):
            self.assertIs(database.connect(), connect.return_value)

        credentials.assert_called_once_with()
        connect.assert_called_once_with(
            host='127.0.0.1', port=16432, dbname='observations',
            user='reader', password='test-password', connect_timeout=15,
        )

    def test_tunnel_uses_database_host_for_netrc_lookup(self):
        with tempfile.TemporaryDirectory() as directory:
            credentials = Path(directory) / 'netrc'
            credentials.write_text('machine db.example.test login tunnel_reader password test-password\n')
            credentials.chmod(0o600)
            database = ConfigurationDB({
                'name': 'observations', 'host': '127.0.0.1', 'port': 16432,
                'netrc_host': 'db.example.test', 'netrc_file': str(credentials), 'auth_method': 'netrc',
            })
            with patch('psycopg.connect') as connect:
                database.connect()
            self.assertEqual(connect.call_args.kwargs['host'], '127.0.0.1')
            self.assertEqual(connect.call_args.kwargs['user'], 'tunnel_reader')

    def test_tunnel_connection_timeout_reports_the_endpoint(self):
        database = ConfigurationDB({'name': 'observations', 'host': '127.0.0.1', 'port': 16432, 'connect_timeout': 5})
        error = ConnectionTimeout('connection timeout expired')
        with (
            patch.object(database, 'get_credentials', return_value=('reader', 'test-password')),
            patch('psycopg.connect', side_effect=error),
            self.assertRaises(ConnectionTimeout) as raised,
        ):
            database.connect()

        self.assertIs(raised.exception, error)
        self.assertIn('127.0.0.1:16432 within 5 seconds', error.__notes__[0])
        self.assertIn('SSH tunnel', error.__notes__[1])


class TestObservationsWorkflow(unittest.TestCase):
    def test_local_and_server_profiles_configure_all_vda_variables(self):
        active = ['rain', 'temperature', 'humidity', 'wind', 'snow', 'radiation']
        inactive = ['discharge', 'soilsat_10', 'soilsat_20', 'soilsat_40']
        for mode in ('local', 'server'):
            with self.subTest(mode=mode):
                settings = workflow.settings_folder / f'app_downloader_workflow_obs_ws_vda_{mode}.json'
                application = json.loads(settings.read_text())['configuration']['application']
                variables = application['source']['variables']
                self.assertEqual(list(variables), active + inactive)
                self.assertEqual(set(variables), set(access_point_vda.VARIABLES))
                self.assertEqual(set(application['destination']), set(variables))
                for name, selection in variables.items():
                    self.assertEqual(selection['variable'].lower(), name)
                    self.assertEqual(selection['geosupervisor'], access_point_vda.VARIABLES[name][2] is not None)
                filtered = ConfigurationDB.filter_application(application)
                self.assertEqual(list(filtered['source']['variables']), active)
                self.assertEqual(list(filtered['destination']), active)

    def test_provider_selection_defaults_to_vda_and_accepts_explicit_name(self):
        self.assertIs(get_provider(), access_point_vda)
        self.assertIs(get_provider(' VDA '), access_point_vda)
        self.assertIs(get_provider('shybox.db_toolkit.access_point_db.access_point_vda'), access_point_vda)
        self.assertIs(get_provider('shybox.db_toolkit.lib_db_vda.access_point'), access_point_vda)

    def test_provider_selection_accepts_package_and_folder_objects(self):
        filename = 'access_point_vda.py'
        locations = (
            {'package': 'shybox.db_toolkit.access_point_db', 'file': filename},
            {'folder': 'shybox/db_toolkit/access_point_db/', 'file': filename},
            {'folder': str(Path(access_point_vda.__file__).parent), 'file': filename},
        )
        for location in locations:
            with self.subTest(location=location):
                self.assertIs(get_provider(location), access_point_vda)

    def test_provider_selection_rejects_invalid_file_field(self):
        with self.assertRaisesRegex(ValueError, r'source\.provider\.file must be a Python filename'):
            get_provider({'package': 'shybox.db_toolkit.access_point_db', 'file': '../access_point_vda.py'})

    def test_unknown_provider_identifies_available_providers(self):
        with self.assertRaisesRegex(ValueError, r"source\.provider 'marche'.*Available providers: vda"):
            get_provider('marche')

    def test_instantaneous_query_template_renders_with_bound_parameters(self):
        query = access_point_vda.timeseries_query('network.station', True, None, 2).as_string()

        self.assertIn('LEFT JOIN "network"."station"', query)
        self.assertIn('t.post_validity_code < 4 OR t.post_validity_code IN (-64, -65)', query)
        self.assertEqual(query.count('%s'), 3)

    def test_entry_point_writes_csv_for_active_variables_only(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            config = json.loads((workflow.settings_folder / 'app_downloader_workflow_obs_ws_vda_local.json').read_text())
            application = config['configuration']['application']
            application['destination']['soilsat_10']['path'] = '{inactive_output_path}'
            application['destination']['rain']['options'].update(delimiter='|', decimal=',', time_format='%H:%M')
            settings = folder / 'config.json'
            settings.write_text(json.dumps(config))
            data = DataFrame([{
                'station_id': 1, 'time': datetime(2026, 11, 25, 12), 'value': 1.25,
                'latitude': 45.5, 'longitude': 7.5, 'station_name': 'Test station', 'post_validity_code': None,
            }])
            environment = {
                'DOMAIN_NAME': 'test', 'PATH_LOG': str(folder / 'logs'), 'PATH_DATA': str(folder / 'data'),
                'TIME_RUN': '2026-11-25 09:00', 'TIME_PERIOD': '1',
            }
            argv = ['downloader', '-settings_file', str(settings), '-time', '2026-11-25 12:00']
            with (
                patch.dict(os.environ, environment),
                patch('sys.argv', argv),
                patch('psycopg.connect') as connect,
                patch(
                    'shybox.db_toolkit.db_handler_base.get_provider', wraps=get_provider,
                ) as select_provider,
                patch.object(ConfigurationDB, 'get_data', return_value=data) as get_data,
            ):
                workflow.main()

            select_provider.assert_called_once_with(application['source']['provider'])
            self.assertTrue((folder / 'logs' / 'test' / config['log']['file_name']).is_file())
            connect.assert_not_called()
            active = ['rain', 'temperature', 'humidity', 'wind', 'snow', 'radiation']
            self.assertEqual([call.args[0] for call in get_data.call_args_list], active * 2)
            for hour in (11, 12):
                with self.subTest(hour=hour):
                    output = folder / 'data' / f'20261125_{hour}'
                    self.assertEqual(
                        {path.name for path in output.glob('*.csv')},
                        {f'weather_stations_{name}_20261125{hour}00.csv' for name in active},
                    )
                    target = output / f'weather_stations_rain_20261125{hour}00.csv'
                    self.assertEqual(target.read_text().splitlines(), [
                        'station|longitude|latitude|code|value|time',
                        'Test station|7,5|45,5|-9999|1,25|12:00',
                    ])
