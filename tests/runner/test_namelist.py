"""Generate and round-trip realistic HMC/S3M namelists without model binaries."""
from pathlib import Path
import tempfile
import unittest

from shybox.runner_toolkit.namelist.namelist_template_handler import NamelistTemplateManager
from shybox.runner_toolkit.namelist.namelist_structure_handler import NamelistStructureManager
from shybox.runner_toolkit.namelist.lib_utils_namelist import parse_fortran_namelist
from shybox.runner_toolkit.namelist.lib_utils_dataclass import Mode, D, M


class NamelistTests(unittest.TestCase):
    def setUp(self):
        self.templates = NamelistTemplateManager()
        self.manager = NamelistStructureManager(self.templates)

    def inputs(self, model, version):
        values = {'sDomainName': 'marche', 'sTimeStart': '202611070000', 'sTimeRestart': '202611062300',
                  'iSimLength': 24, 'iDtModel': 3600, 'iFlagRestart': 0, 'iFlagSnowAssim': 0,
                  'a1dGeoForcing': [43., 12.], 'a1dResForcing': [.01, .01], 'a1iDimsForcing': [3, 4]}
        for params in self.templates.get(model, version).values():
            for name, var in params.items():
                if var.mode == Mode.MANDATORY and name not in values:
                    if name.startswith('sPath'): values[name] = '/tmp/model/$yyyy/$mm/'
                    elif name.startswith('iDt'): values[name] = 3600
        return values

    def test_all_registered_model_versions_round_trip(self):
        for model, version in [('hmc','3.1.6'),('hmc','3.2.0'),('hmc','3.3.0'),('s3m','5.3.3')]:
            with self.subTest(model=model, version=version):
                text = self.manager.to_fortran(model, version, self.inputs(model, version))
                parsed = parse_fortran_namelist(text)
                section = model.upper() + '_Namelist'
                self.assertEqual(parsed[section]['iSimLength'], 24)
                self.assertEqual(parsed[section]['iDtModel'], 3600)
                self.assertEqual(parsed[section]['sTimeStart'], '202611070000')
                self.assertEqual(parsed[section]['sPathData_Forcing_Gridded'], '/tmp/model/$yyyy/$mm/')
                if model == 's3m': self.assertEqual(parsed[section]['a1iDimsForcing'], [3, 4])

    def test_missing_mandatory_fields_fail_for_both_models(self):
        for model, version in [('hmc','3.3.0'),('s3m','5.3.3')]:
            with self.subTest(model=model), self.assertRaisesRegex(ValueError, 'sDomainName'):
                self.manager.to_fortran(model, version, {})

    def test_explicit_values_override_patterns_and_cast_numbers(self):
        updates = self.manager.build_updates('hmc','3.3.0',
            by_value={'iDtModel':'1800','dUc':'2.5'},
            by_pattern={'time_steps':{'active':True,'template':'iDt','value':'3600'}})
        self.assertEqual(updates['HMC_Namelist']['iDtModel'], 1800)
        self.assertEqual(updates['HMC_Namelist']['iDtData_Forcing'], 3600)
        self.assertEqual(updates['HMC_Parameters']['dUc'], 2.5)

    def test_generated_file_preserves_existing_file_unless_overwrite(self):
        obj = self.manager.to_fortran('hmc','3.3.0',self.inputs('hmc','3.3.0'),as_object=True)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'member'/'hmc.namelist'
            obj.write_to_ascii(str(path))
            self.assertEqual(parse_fortran_namelist(path.read_text())['HMC_Namelist']['iSimLength'],24)
            path.write_text('existing')
            obj.write_to_ascii(str(path))
            self.assertEqual(path.read_text(),'existing')
            obj.write_to_ascii(str(path),overwrite=True)
            self.assertIn('&HMC_Namelist',path.read_text())

    def test_template_defaults_do_not_leak_between_builds(self):
        first = self.manager.build_values('s3m','5.3.3')
        first['S3M_Snow']['a1dAltRange'][0] = -123
        self.assertEqual(self.manager.build_values('s3m','5.3.3')['S3M_Snow']['a1dAltRange'][0],1500)

    def test_boolean_array_and_exponent_fortran_round_trip(self):
        template = NamelistTemplateManager(templates={('test','1'):{'Control':{'enabled':D(True),'weights':D([1.,2.]),'domain':M()}}})
        text = NamelistStructureManager(template).to_fortran('test','1',{'domain':'marche'})
        parsed = parse_fortran_namelist(text)['Control']
        self.assertIs(parsed['enabled'],True)
        self.assertEqual(parsed['weights'],[1.,2.])
        self.assertEqual(parse_fortran_namelist('&N\n x=1.2D+03\n/')['N']['x'],1200.)

    def test_unknown_model_version_is_rejected(self):
        with self.assertRaises(KeyError):self.templates.get('hmc','0.0.0')


if __name__ == '__main__': unittest.main()
