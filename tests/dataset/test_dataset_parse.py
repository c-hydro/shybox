import datetime as dt
import unittest

from shybox.dataset_toolkit.lib_dataset_parse import get_date_from_str, substitute_string


class TestDatasetTagSubstitution(unittest.TestCase):
    def test_substitutes_text_and_numbers(self):
        tags = {"member": "001", "domain": "marche", "year": "2026", "count": 4, "scale": 1.25}
        template = "{domain}/run_{member}/{year}/{count}/{scale}/{missing}"
        self.assertEqual(substitute_string(template, tags), "marche/run_001/2026/4/1.25/{missing}")
        self.assertEqual(substitute_string("{domain:>8}/{count:03d}/{scale:.1f}", tags), "  marche/004/1.2")

    def test_formats_datetime_and_date_strings(self):
        for value in (dt.datetime(2026, 11, 18, 12, 30), "2026-11-18 12:30",
                      "2026-11-18T12:30:00Z", "202611181230", "20261118123000"):
            with self.subTest(value=value):
                self.assertEqual(substitute_string("{run:%Y%m%d%H%M}", {"run": value}), "202611181230")
        self.assertEqual(substitute_string("{day:%Y-%m-%d}", {"day": "20261118"}), "2026-11-18")

    def test_preserves_date_text_without_a_format(self):
        value = "2026-11-18T12:30:00Z"
        self.assertEqual(substitute_string("{run}", {"run": value}), value)

    def test_invalid_date_can_still_be_formatted_as_text(self):
        value = "2026-02-30"
        with self.assertRaises(ValueError):
            get_date_from_str(value)
        self.assertEqual(substitute_string("{run:>12}", {"run": value}), "  2026-02-30")

    def test_expands_member_ids_without_changing_leading_zeros(self):
        self.assertEqual(substitute_string("run_{member}", {"member": ["001", "002"]}), ["run_001", "run_002"])
