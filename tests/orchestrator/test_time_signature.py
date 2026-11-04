"""Endpoint selection for file-grouped and step-grouped workflow times."""
import unittest

import pandas as pd

from shybox.orchestrator_toolkit.lib_orchestrator_process import _parser_time_signature


class TestTimeSignature(unittest.TestCase):
    def setUp(self):
        self.times = pd.date_range('2026-11-03', periods=4, freq='h', tz='Europe/Rome')

    def test_start_and_end_unwrap_file_grouped_index(self):
        for values in ([self.times], self.times, list(self.times), [[self.times]]):
            with self.subTest(values=type(values).__name__):
                self.assertEqual(_parser_time_signature(values, 'start'), self.times[0])
                self.assertEqual(_parser_time_signature(values, 'end'), self.times[-1])

    def test_range_signatures_return_timestamp_endpoints(self):
        for signature in ('range', 'start/end', 'period'):
            with self.subTest(signature=signature):
                self.assertEqual(_parser_time_signature([self.times], signature),
                                 (self.times[0], self.times[-1]))

    def test_step_and_current_preserve_original_objects(self):
        for signature in ('step', 'current'):
            for values in ([self.times], self.times, []):
                with self.subTest(signature=signature):
                    self.assertIs(_parser_time_signature(values, signature), values)

    def test_empty_index_inside_list_is_rejected(self):
        for values in ([], pd.DatetimeIndex([]), [pd.DatetimeIndex([])]):
            for signature in ('start', 'end', 'range', 'period', 'start/end'):
                with self.subTest(signature=signature), self.assertRaisesRegex(ValueError, 'cannot be empty'):
                    _parser_time_signature(values, signature)

    def test_unknown_signature_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Unknown time signature'):
            _parser_time_signature([self.times], 'unknown')


if __name__ == '__main__':
    unittest.main()
