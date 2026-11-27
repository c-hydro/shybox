"""Test CSV quality codes from the VDA observation provider."""

import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from shybox.db_toolkit.access_point_db import access_point_vda


class TestVdaHourlyCodes(unittest.TestCase):
    def test_csv_includes_station_metadata_and_uses_only_finite_sql_codes(self):
        first = datetime(2026, 11, 25, 12)
        second = datetime(2026, 11, 25, 13)
        third = datetime(2026, 11, 25, 14)
        fourth = datetime(2026, 11, 25, 15)
        source = MagicMock()
        cases = (
            ('temperature', 1, [(first, 1.5), (second, None)], [('0', '1.5'), ('0', '-9999')]),
            ('rain', 7, [(first, 1.5)], [('0', '1.5')]),
            ('snow', 8,
             [(first, -64, 1.5), (second, None, None), (third, float('nan'), 2.5), (fourth, float('inf'), 3.5)],
             [('-64', '1.5'), ('0', '-9999'), ('0', '2.5'), ('0', '3.5')]),
        )
        for variable, parameter, readings, expected in cases:
            with self.subTest(variable=variable), tempfile.TemporaryDirectory() as folder:
                station = (1, parameter, 9, 'Test station', 'tbls_vda.station', 'unit', True, 'A', 1000, 45.5, 7.1)
                selection = {'variable': variable, 'networks': ['cf-vda']}
                with (
                    patch.object(access_point_vda, 'read_rows', side_effect=[[station], readings]),
                    patch('shybox.logging_toolkit.lib_logging_utils._get_default_log'),
                ):
                    data = access_point_vda.get_data(source, selection, first, readings[-1][0])

                destination = {
                    'path': folder, 'file_name': 'observations.csv',
                    'options': {'delimiter': ';', 'missing_value': -9999},
                }
                access_point_vda.write_data(data, destination, variable=variable)

                with (Path(folder) / destination['file_name']).open(newline='') as stream:
                    reader = csv.DictReader(stream, delimiter=';')
                    self.assertEqual(reader.fieldnames, [
                        'station', 'longitude', 'latitude', 'code', 'value', 'time',
                        'station_id', 'station_name', 'altitude', 'is_ott', 'is_heated',
                        'network', 'alert_area', 'unit', 'post_validity_code', 'variable',
                    ])
                    rows = list(reader)
                self.assertEqual([(row['code'], row['value']) for row in rows], expected)
                self.assertEqual([row['post_validity_code'] for row in rows], [code for code, _ in expected])
                self.assertEqual(rows[0]['station'], rows[0]['station_name'])
                self.assertEqual(
                    [rows[0][key] for key in ('station_id', 'altitude', 'network', 'alert_area', 'unit', 'variable')],
                    ['1', '1000', 'cf-vda', 'A', 'unit', variable],
                )
