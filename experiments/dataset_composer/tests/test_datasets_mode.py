"""Dataset mode defaults, member identifiers and source declarations."""
import unittest

from shybox.runner_toolkit.datasets.datasets_mode import DatasetMode


class TestDatasetMode(unittest.TestCase):
    def test_default_is_deterministic_with_deterministic_member(self):
        self.assertEqual(DatasetMode().as_dict(), {'mode': 'deterministic', 'member': 'deterministic'})
        self.assertFalse(DatasetMode().is_probabilistic)
        self.assertFalse(DatasetMode().has_member)

    def test_probabilistic_member_preserves_leading_zeros(self):
        mode = DatasetMode('probabilistic', '001')
        self.assertTrue(mode.is_probabilistic)
        self.assertTrue(mode.has_member)
        self.assertEqual(mode.member, '001')
        self.assertEqual(mode.run_mode, 'probabilistic_ensemble')
        self.assertEqual(mode.for_member('002').member, '002')
        self.assertEqual(mode.member, '001')

    def test_undeclared_member_keeps_the_default_label(self):
        self.assertEqual(DatasetMode('probabilistic').member, 'deterministic')
        self.assertFalse(DatasetMode('probabilistic').has_member)
        self.assertEqual(DatasetMode.from_source({}).member, 'deterministic')
        self.assertEqual(DatasetMode.from_source({'mode': 'probabilistic', 'member': '005'}).member, '005')

    def test_mode_alias_and_invalid_combinations(self):
        self.assertEqual(DatasetMode('probabilistic_ensemble', '010').mode, 'probabilistic')
        for mode, member in (('bad', None), ('deterministic', '001'), ('probabilistic', '')):
            with self.subTest(mode=mode, member=member), self.assertRaises(ValueError):
                DatasetMode(mode, member)


if __name__ == '__main__':
    unittest.main()
