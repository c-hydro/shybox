# HMC time-series analyzer configurations

The example configurations distinguish the run mode and output format:

| Configuration | Run mode | Output |
| --- | --- | --- |
| `app_analyzer_workflow_hmc_time_series_destine_marche_deterministic_netcdf.json` | Deterministic | One NetCDF file |
| `app_analyzer_workflow_hmc_time_series_destine_marche_deterministic_json.json` | Deterministic | One JSON file per section |
| `app_analyzer_workflow_hmc_time_series_destine_marche_probabilistic_netcdf.json` | Probabilistic ensemble | Planned ensemble NetCDF |
| `app_analyzer_workflow_hmc_time_series_destine_marche_probabilistic_json.json` | Probabilistic ensemble | Planned ensemble JSON per section |

The two previous configurations ending in `_marche_netcdf.json` and
`_marche_json.json` have been renamed to their deterministic counterparts.

All four files configure the run identity at
`application.dynamic_data_dst.file_args.run_metadata`:

```json
{
  "run_domain": "marche",
  "run_mode": "deterministic",
  "run_name": "nwp_ecmwf0100_realtime_marche",
  "run_var": "deterministic"
}
```

The probabilistic configurations use `run_mode: "probabilistic_ensemble"`,
`run_name: "rfarm_icon-2i_realtime_marche"`, and a comma-separated `run_var`
listing members `001` through `030`. This list is the intended source of member
identities; preserve its ordering and leading zeros. Adapt it to your experiment.

For probabilistic inputs, `PATH_SRC_HMC` should point to the runner's
`model_output` folder. Both simulated discharge paths follow the probabilistic
runner's `run_{ensemble_member}/time_series/` layout. The member placeholder must
be filled separately for each declared member. Observation inputs and section
registries are shared. Probabilistic outputs and scratch paths use a separate
`probabilistic_ensemble` folder, and each mode/format has its own log filename.
Deterministic source/output paths retain the previous layout.

For JSON, the explicit variable mapping selects analysis as the primary
`time_series_discharge_simulated` series, and exports open-loop discharge under
`time_series_discharge_simulated_openloop`. The planned ensemble writer should
append each member ID to member-dependent simulated fields, as in the supplied
legacy ensemble examples. Change the mapping explicitly if open-loop is your
primary product. Shared observations have no member suffix.

## Current implementation boundary

These changes configure and name the four examples; they do not implement
ensemble processing. `app_analyzer_workflow_hmc_time_series_main.py` currently
reads deterministic inputs, the JSON wrapper accepts two-level
`(variable, section)` columns, and the NetCDF time-series writer handles
two-dimensional variables. The probabilistic files are templates for the next
analyzer/writer update, not runnable ensemble workflows yet.

The deterministic JSON writer already accepts `file_args.run_metadata`.
Writing the configured metadata as NetCDF global attributes still needs to be
wired into the NetCDF writer. The future ensemble path should preserve all four
metadata fields, the expected member list, missing-member availability and one
shared observation series per section.
