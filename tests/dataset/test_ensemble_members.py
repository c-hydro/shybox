import unittest

from shybox.generic_toolkit.lib_utils_ensemble import generate_ensemble_list


class TestEnsembleMembers(unittest.TestCase):
    def test_generates_members_from_configuration(self):
        settings = {
            "ensemble_start": 1,
            "ensemble_end": 4,
            "ensemble_step": 1,
            "ensemble_format": "%03d",
        }
        self.assertEqual(generate_ensemble_list(**settings), ["001", "002", "003", "004"])

    def test_step_and_supported_formats(self):
        for member_format in ("%03d", "03d", "{:03d}"):
            with self.subTest(member_format=member_format):
                self.assertEqual(generate_ensemble_list(1, 5, 2, member_format), ["001", "003", "005"])
                self.assertEqual(generate_ensemble_list(1, 4, 2, member_format), ["001", "003"])

    def test_includes_a_single_member(self):
        self.assertEqual(generate_ensemble_list(4, 4), ["004"])

    def test_rejects_invalid_settings(self):
        for args in ((-1, 4), (4, 1), (1, 4, 0), (1, 4, -1), (True, 4), (1, 4, 1.5),
                     (1, 4, 1, "%invalid"), (1, 4, 1, "{:x} out"), (1, 4, 1, "")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                generate_ensemble_list(*args)
