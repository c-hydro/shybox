import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from netCDF4 import Dataset
import pandas as pd

from shybox.dataset_toolkit.lib_dataset_generic import write_to_file
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.orchestrator_toolkit.lib_orchestrator_process import _filter_output_collections, _group_tag
from shybox.orchestrator_toolkit.lib_orchestrator_utils_discovery import load_process
from shybox.orchestrator_toolkit.orchestrator_handler_config import OrchestratorConfig


class TestTimeSeriesCollections(unittest.TestCase):
    def setUp(self):
        self.logger = LoggingManager(name="time_series_collection_tests", level=50)
        self.enterContext(self.logger.use_as_current())
        self.registry = pd.DataFrame({"tag": ["a", "b"], "id": [1, 2]})
        self.registry.attrs["type"] = {"tag": str, "id": int}
        self.times = pd.date_range("2026-11-18", periods=2, freq="h")
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def frame(self, first, second):
        return pd.DataFrame({"time": self.times, "a": first, "b": second})

    def run_process(self, name, data, **kwargs):
        process = load_process(name)
        return process(
            _group_tag(data), sections_hmc=self.registry, sections_db=self.registry,
            variable_in={
                "data": {"mapping": {"data|rain": "rain", "data|obs": "rain_obs"}},
            },
            variable_out={"results": {"mapping": {"data": "time_series"}}},
            **kwargs,
        )["time_series"]

    def test_deterministic_process_keeps_two_levels(self):
        result = self.run_process("join_time_series_deterministic", {
            "data|rain@time|0": self.frame([1, 2], [3, 4]),
        })
        self.assertEqual(result.attrs["datasets"], ["rain"])
        self.assertEqual(result.attrs["members"], "deterministic")
        self.assertEqual(list(result.columns), [("rain", "a"), ("rain", "b")])
        self.assertEqual(result["rain"]["b"].tolist(), [3, 4])

    def test_ensemble_writes_to_netcdf_and_json(self):
        result = self.run_process("join_time_series_probabilistic", {
            "data|rain::data|3@time|0": self.frame([11, 12], [13, 14]),
            "data|rain::data|1@time|0": self.frame([1, 2], [3, 4]),
            "data|obs@time|0": self.frame([5, 6], [7, 8]),
        }, ensemble_members=["001", "004", "007"], ensemble_variables=["rain"])
        self.assertEqual(result.attrs["members"], "001;004;007")
        self.assertEqual(result.columns.names, ["variable", "section", "member"])
        self.assertEqual(result["rain", "a", "004"].tolist(), [-9999, -9999])
        self.assertEqual(result["rain_obs", "b", ""].tolist(), [7, 8])

        mapping = {"rain": "rain_simulated", "rain_obs": "rain_observed"}
        selected = _filter_output_collections({"time_series": result}, mapping)["time_series"]
        nc_path = self.root / "ensemble.nc"
        write_to_file(
            {"time_series": selected}, str(nc_path), file_format="netcdf", file_type="time_series_hmc",
            map=mapping, units={"rain": "mm"}, run_metadata={"run_mode": "probabilistic_ensemble"},
        )
        with Dataset(nc_path) as nc:
            self.assertEqual(nc["ensemble"][:].tolist(), ["001", "004", "007"])
            self.assertEqual(nc.run_var, "001,004,007")
            self.assertEqual(nc["rain_simulated"].dimensions, ("time", "sections", "ensemble"))
            self.assertEqual(nc["rain_simulated"].units, "mm")
            self.assertEqual(nc["rain_simulated"][:, 1, 2].tolist(), [13, 14])
            self.assertTrue(nc["rain_simulated"][:, :, 1].mask.all())
            self.assertEqual(nc["rain_observed"].dimensions, ("time", "sections"))

        write_to_file(
            selected, str(self.root / "{section}.json"), file_format="json",
            file_type="time_series_hmc", map=mapping,
        )
        section = json.loads((self.root / "a.json").read_text())
        self.assertEqual(section["run_var"], "001,004,007")
        self.assertEqual(section["time_series_rain_simulated_001"], "1.0,2.0")
        self.assertEqual(section["time_series_rain_simulated_004"], "-9999.0,-9999.0")
        self.assertEqual(section["time_series_rain_simulated_007"], "11.0,12.0")
        self.assertEqual(section["time_series_rain_observed"], "5.0,6.0")

    def test_preserves_the_member_axis_for_a_single_configured_member(self):
        result = self.run_process("join_time_series_probabilistic", {
            "data|rain@time|0": self.frame([1, 2], [3, 4]),
        }, ensemble_members=["004"], ensemble_variables=["rain"], ensemble_format="02d")
        self.assertEqual(list(result.columns), [("rain", "a", "004"), ("rain", "b", "004")])

    def test_infers_formatted_members_from_data_positions(self):
        rain = self.frame([1, 2], [3, 4])
        observations = self.frame([5, 6], [7, 8])
        result = self.run_process("join_time_series_probabilistic", {
            "data|rain::data|3@time|0": self.frame([11, 12], [13, 14]),
            "data|rain::data|1@time|0": rain.iloc[:1],
            "data|rain::data|1@time|1": rain.iloc[1:],
            "data|obs@time|0": observations.iloc[:1],
            "data|obs@time|1": observations.iloc[1:],
        }, ensemble_format="{:02d}")
        self.assertEqual(result.attrs["members"], "01;02;03")
        self.assertEqual([member for variable, section, member in result.columns
                          if (variable, section) == ("rain", "a")], ["01", "02", "03"])
        self.assertEqual(result["rain", "a", "01"].tolist(), [1, 2])
        self.assertEqual(result["rain", "a", "02"].tolist(), [-9999, -9999])
        self.assertEqual(result["rain", "a", "03"].tolist(), [11, 12])
        self.assertEqual(result["rain_obs", "b", ""].tolist(), [7, 8])

    def test_time_only_groups_have_no_inferred_members(self):
        observations = self.frame([5, 6], [7, 8])
        result = self.run_process("join_time_series_probabilistic", {
            "data|obs@time|0": observations.iloc[:1],
            "data|obs@time|1": observations.iloc[1:],
        })
        self.assertEqual(result.attrs["members"], "")
        self.assertEqual(list(result.columns), [("rain_obs", "a", ""), ("rain_obs", "b", "")])
        self.assertEqual(result["rain_obs", "b", ""].tolist(), [7, 8])

    def test_runtime_arguments_override_configured_values(self):
        process = load_process("join_time_series_probabilistic")
        (self.root / "rain.txt").touch()
        options = {"path": str(self.root), "file_type": "time_series_hmc", "data_layout": "time_series",
                   "file_workflow": "SERIES", "time_signature": "step", "logger": self.logger}
        source = DataLocal(file_name="rain.txt", file_format="ascii", file_io="input",
                           file_variable="RAIN", variable_template={"vars_data": {"RAIN": "rain"}}, **options)
        cases = (({}, "001"), ({"ensemble_members": ["004"]}, "004"))
        for selection, expected in cases:
            with self.subTest(selection=selection):
                destination = DataLocal(file_name=f"{expected}_{{section}}.json", file_format="json",
                                        file_io="output", file_variable="SERIES_OUT",
                                        variable_template={"vars_data": {"rain": "rain"}}, **options)
                orchestrator = OrchestratorConfig.configure(
                    {"rain": source}, {"output": destination},
                    data_ref={"sections_hmc": self.registry, "sections_db": self.registry},
                    configuration={
                        "options": {
                            "intermediate_output": "Tmp", "tmp_dir": str(self.root / "tmp"),
                            "io_config": {"mode": "many_data_ts_to_multiple_files"},
                        },
                        "process_list": {"SERIES": [{
                            "function": process.__name__,
                            "datasets": {"data|rain": "rain"},
                            "results": {"data": "time_series"},
                            "args": {"ensemble_members": ["001"], "ensemble_variables": ["rain"]},
                        }]},
                    },
                    logger=self.logger,
                )
                self.assertEqual(orchestrator.args_run, {})
                orchestrator.args_run["stale"] = True
                with (
                    patch.object(source, "is_readable", return_value=("available", True)),
                    patch.object(source, "get_data", return_value=self.frame([1, 2], [3, 4])) as get_data,
                    patch.object(orchestrator, "run_single_ts", wraps=orchestrator.run_single_ts) as run_step,
                ):
                    orchestrator.run(time=self.times[0], debug=True, **selection)

                runtime = run_step.call_args.kwargs
                self.assertEqual(runtime.get("ensemble_members"), selection.get("ensemble_members"))
                self.assertIs(runtime["debug"], True)
                self.assertEqual(orchestrator.args_run, {"debug": True, **selection})
                self.assertNotIn("ensemble_members", get_data.call_args.kwargs)
                self.assertNotIn("_args_run", get_data.call_args.kwargs)

                section = json.loads((self.root / f"{expected}_a.json").read_text())
                self.assertEqual(section["run_var"], expected)
