# HMC time-series analyzer

Run the analyzer from the repository root with the SHYBOX scientific
environment active. Select the JSON profile that matches your mode and output
format.

| Run mode | Entry point under `workflow/analyzer` | Profile suffix |
| --- | --- | --- |
| Deterministic | `app_analyzer_workflow_hmc_time_series_main_deterministic.py` | `_deterministic_netcdf.json` or `_deterministic_json.json` |
| Probabilistic | `app_analyzer_workflow_hmc_time_series_main_probabilistic.py` | `_probabilistic_netcdf.json` or `_probabilistic_json.json` |

```bash
python -m workflow.analyzer.app_analyzer_workflow_hmc_time_series_main_probabilistic \
  -settings_file workflow/analyzer/app_analyzer_workflow_hmc_time_series_destine_marche_probabilistic_netcdf.json \
  -time "2026-08-11 12:00"
```

In probabilistic profiles, `application.ensemble` sets the member range, step
and ID format. The configured `run_{ensemble_member}` source path is resolved
for each member. Section registries and observations are shared, and missing
member files are represented by no-data values.

NetCDF stores ensemble simulations on `(time, sections, ensemble)` and other
variables on `(time, sections)`. The `index_start` and `index_end` attributes
name arrays of per-section offsets for slicing flattened columns. JSON writes
one hydrograph file per section by default, with member IDs appended to
simulated field names and shared observations written once per section.

See the [workflow guide](../../docs/workflows.rst) for environment setup and
output examples.
