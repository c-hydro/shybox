"""Tests for the CVA provider and its hourly CSV workflow."""

import csv
import os
import tempfile
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from shybox.db_toolkit.access_point_db import access_point_cva
from shybox.db_toolkit.db_handler_base import ConfigurationDB
from workflow.downloader import app_downloader_workflow_cva_obs_dam_main as workflow


class TestCvaWorkflow(unittest.TestCase):
    def test_local_and_server_settings_write_hourly_csvs(self):
        for profile in ('local', 'server'):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                self.check_profile(profile, Path(directory))

    def check_profile(self, profile, folder):
        hour = datetime(2026, 11, 25, 12)
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        station = (4, 9, 'Test dam', 'tables_cva.dam', 'm', 'test-zone', 1800, 45.5, 7.1)
        cursor.fetchall.side_effect = [[station], [(hour, Decimal('5.25'))], [station], [(hour, None)]]
        settings = Path(workflow.__file__).with_name(f'app_downloader_workflow_cva_obs_dam_{profile}.json')
        environment = {
            'DOMAIN_NAME': 'test', 'PATH_LOG': str(folder / 'logs'), 'PATH_DATA': str(folder / 'data'),
            'TIME_RUN': '2026-11-25 12:00', 'TIME_PERIOD': '0',
        }
        argv = ['downloader', '-settings_file', str(settings), '-time', environment['TIME_RUN']]
        with (
            patch.dict(os.environ, environment),
            patch('sys.argv', argv),
            patch.object(ConfigurationDB, 'connect', return_value=connection),
        ):
            workflow.main()

        metadata, levels, discharge_metadata, discharge = cursor.execute.call_args_list
        self.assertEqual(metadata.args, (access_point_cva.STATION_QUERY, ('tables_cva', 100)))
        self.assertEqual(discharge_metadata.args, (access_point_cva.STATION_QUERY, ('tables_cva', 101)))
        self.assertNotIn('station_active', metadata.args[0])
        for call in (levels, discharge):
            query, parameters = call.args
            self.assertIn('avg(CASE WHEN t.post_validity_code >= -1', query.as_string())
            self.assertIn('LEFT JOIN "tables_cva"."dam"', query.as_string())
            self.assertEqual(parameters, (hour, hour, 9))

        for variable, value in (('reservoir_water_level', '5.25'), ('turbine_discharge', '-9999')):
            output = folder / 'data' / '20261125_12' / f'cva_{variable}_202611251200.csv'
            with output.open(newline='') as stream:
                row, = csv.DictReader(stream, delimiter=';')
            self.assertEqual(row['station_name'], 'Test dam')
            self.assertEqual(row['time'], '2026-11-25 12:00:00')
            self.assertEqual(row['value'], value)

    def test_geographic_bounds_exclude_stations_outside_the_area(self):
        source = MagicMock()
        cursor = source.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(4, 9, 'Test dam', 'tables_cva.dam', 'm', None, None, 45.5, 7.1)]
        configuration = {'variable': 'TURBINATE', 'geolimits': [46, None, None, None]}
        hour = datetime(2026, 11, 25, 12)

        result = access_point_cva.get_data(source, configuration, hour, hour)

        self.assertTrue(result.empty)
        self.assertEqual(result.columns.tolist(), access_point_cva.COLUMNS)
        cursor.execute.assert_called_once()
