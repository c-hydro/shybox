"""Exercise actual NetCDF readers and HTTP publication for both run modes."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd
import xarray as xr

from hydrapp.app import create_app
from hydrapp.data import Catalog, load_config


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.times = pd.date_range("2026-08-11 11:00", periods=3, freq="h")
        self.metadata = {
            "section_name": ("sections", ["First", "Missing"]),
            "basin": ("sections", ["Foglia", "Metauro"]),
            "longitude": ("sections", [12.9, 13.1]),
            "latitude": ("sections", [43.8, 43.2]),
            "threshold_level_1": ("sections", [10., -9999.]),
            "threshold_level_2": ("sections", [20., -9999.]),
            "threshold_level_3": ("sections", [30., -9999.]),
        }
        self.det = xr.Dataset({**self.metadata,
            "discharge_simulated_analysis": (("time", "sections"), [[999., -9999.], [5., np.nan], [15., np.inf]]),
            "discharge_observed": (("time", "sections"), np.full((3, 2), np.nan)),
        }, coords={"time": self.times})
        values = np.full((3, 2, 4), np.nan)
        values[:, 0, :] = [[0., 0., 0., 0.], [0., 11., 21., 31.], [0., np.nan, np.nan, np.nan]]
        self.prob = xr.Dataset({**self.metadata,
            "discharge_simulated_analysis": (("time", "sections", "member"), values),
            "discharge_observed": (("time", "sections"), np.full((3, 2), 1000.)),
        }, coords={"time": self.times, "member": [1, 2, 3, 4]})
        for folder, ds in (("det", self.det), ("prob", self.prob)):
            (self.root / folder).mkdir()
            ds.to_netcdf(self.root / folder / "hmc.series_marche_202608111200.nc")
        source = {"folder": "det", "file_pattern": "**/*.nc",
                  "run_time_regex": r"(?P<time>\d{12})\.nc$", "run_time_format": "%Y%m%d%H%M"}
        self.config = {"default_run": "det", "overview": {"period": "all", "probability_cutoff": .5},
            "runs": {
                "det": {"name": "Deterministic", "mode": "deterministic", "time_series": source},
                "prob": {"name": "Probabilistic", "mode": "probabilistic", "ensemble_dimension": "member",
                         "time_series": {**source, "folder": "prob"}},
                "empty": {"mode": "deterministic", "time_series": {**source, "folder": "missing"}},
                "disabled": {"mode": "deterministic", "enabled": False, "time_series": source},
            }}
        self.config_path = self.root / "config.json"
        self.build()

    def build(self):
        self.config_path.write_text(json.dumps(self.config))
        self.app = create_app(self.config_path)
        self.app.testing = True
        self.client = self.app.test_client()
        self.collections = {c["id"]: c for c in self.client.get("/api/runs").json["collections"]}

    def endpoint(self, collection="det"):
        return f'/api/runs/{collection}/{self.collections[collection]["runs"][0]["id"]}'

    def test_declared_only_enabled_and_empty_collections(self):
        self.assertEqual(set(self.collections), {"det", "prob", "empty"})
        self.assertEqual(self.collections["empty"]["runs"], [])
        self.assertEqual(self.collections["det"]["runs"][0]["time"], "2026-08-11T12:00:00")

    def test_deterministic_status_and_forecast_window(self):
        all_times = self.client.get(self.endpoint()).json
        self.assertEqual(all_times["sections"][0]["status"], "red")
        forecast = self.client.get(self.endpoint() + "?period=forecast").json
        self.assertEqual(forecast["sections"][0]["peak"], 15.)
        self.assertEqual(forecast["sections"][0]["status"], "yellow")
        self.assertEqual(forecast["sections"][1]["status"], "unknown")
        self.assertEqual(forecast["sections"][1]["thresholds"], [None] * 3)

    def test_probabilities_and_observations_are_not_members(self):
        payload = self.client.get(self.endpoint("prob")).json
        self.assertEqual(payload["sections"][0]["probabilities"], [.75, .5, .25])
        self.assertEqual(payload["sections"][0]["status"], "orange")
        self.assertEqual(payload["sections"][0]["peak"], 31.)
        self.assertEqual(payload["sections"][1]["status"], "unknown")

    def test_probability_denominator_excludes_wholly_missing_members(self):
        self.prob["discharge_simulated_analysis"].values[:, 0, 0] = np.nan
        self.prob.to_netcdf(self.root / "prob/hmc.series_marche_202608111200.nc", mode="w")
        probabilities = self.client.get(self.endpoint("prob")).json["sections"][0]["probabilities"]
        np.testing.assert_allclose(probabilities, [1., 2/3, 1/3])

    def test_deterministic_curve_serializes_missing_values_as_null(self):
        response = self.client.get(self.endpoint() + "/sections/1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["traces"][0]["values"], [None] * 3)
        self.assertNotIn("NaN", response.get_data(as_text=True))
        self.assertNotIn("Infinity", response.get_data(as_text=True))

    def test_ensemble_quantiles_are_actual_member_quantiles(self):
        payload = self.client.get(self.endpoint("prob") + "/sections/0").json
        trace = payload["traces"][0]
        self.assertEqual(trace["kind"], "ensemble")
        self.assertEqual(trace["values"][2], [0., 16., 0.])
        self.assertEqual(trace["member_counts"], [4, 4, 1])
        missing = self.client.get(self.endpoint("prob") + "/sections/1").json
        self.assertEqual(missing["traces"][0]["values"][2], [None] * 3)

    def test_invalid_ensemble_dimension_rejected(self):
        self.config["runs"]["prob"]["ensemble_dimension"] = "ensemble"
        self.build()
        response = self.client.get(self.endpoint("prob"))
        self.assertEqual(response.status_code, 422)
        self.assertIn("expected dimensions", response.json["error"])

    def test_run_section_and_query_validation(self):
        self.assertEqual(self.client.get("/api/runs/det/unknown").status_code, 404)
        self.assertEqual(self.client.get(self.endpoint() + "/sections/999").status_code, 404)
        self.assertEqual(self.client.get(self.endpoint() + "?period=wrong").status_code, 400)
        self.assertEqual(self.client.get(self.endpoint() + "?source=wrong").status_code, 400)
        empty = self.client.get(self.endpoint() + "?source=discharge_simulated_openloop")
        self.assertEqual(empty.status_code, 422)

    def test_malformed_file_is_reported_and_new_runs_are_discovered(self):
        (self.root / "det/bad.nc").touch()
        self.det.to_netcdf(self.root / "det/hmc.series_marche_202608121200.nc")
        collection = self.client.get("/api/runs").json["collections"][0]
        self.assertEqual(len(collection["runs"]), 2)
        self.assertEqual(len(collection["issues"]), 1)
        self.assertEqual(collection["runs"][0]["date"], "2026-08-12")

    def test_threshold_comparison_is_strict_and_zero_is_valid(self):
        self.det["discharge_simulated_analysis"].values[:, 0] = [0., 10., 10.]
        self.det.to_netcdf(self.root / "det/hmc.series_marche_202608111200.nc", mode="w")
        self.assertEqual(self.client.get(self.endpoint()).json["sections"][0]["status"], "green")

    def test_invalid_configuration_is_rejected(self):
        for field, value in (("mode", "anything"),):
            cfg = copy.deepcopy(self.config)
            cfg["runs"]["det"][field] = value
            self.config_path.write_text(json.dumps(cfg))
            with self.assertRaises(ValueError):
                load_config(self.config_path)
        cfg = copy.deepcopy(self.config)
        cfg["runs"]["det"]["time_series"]["file_pattern"] = "../*.nc"
        self.config_path.write_text(json.dumps(cfg))
        with self.assertRaises(ValueError):
            load_config(self.config_path)

    def test_pages_and_local_assets(self):
        for path in ("/", "/time-series", "/maps", "/static/dashboard.js", "/static/dashboard.css", "/static/img/app_logo_fcima.png"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                response.close()
        self.assertIn(b"Gridded results are not available yet", self.client.get("/maps").data)


if __name__ == "__main__":
    unittest.main()
