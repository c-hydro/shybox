# SHYBOX — Snow and Hydrologic Modeling Toolbox

[![License](https://img.shields.io/badge/license-EUPL--1.2-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/environment-Python%203.12-blue)](requirements_shybox_base.yaml)
[![Documentation Status](https://readthedocs.org/projects/shybox/badge/?version=latest)](https://shybox.readthedocs.io/en/latest/)

**SHYBOX** (Snow and HYdrologic toolBOX) provides Python tools and JSON-configured workflows for preparing environmental datasets, running **HMC / Continuum** and **S3M**, and analyzing model results. It is developed by the **CIMA Research Foundation** for research and operational hydrological and snow forecasting.

Prepare upstream products, compose observed/forecast forcing, execute a model, then analyze and monitor its results. Shared configuration, time, logging and processing components connect these stages.

Package version: **1.6.0**. This README documents both published features and development interfaces. The `main` branch contains the dataset planner, conversion, analysis, base runners under `workflow/runner/` and the Flask viewer under `dashboard/`. Hydrapp and monitoring are available on `fproofs`; the new forcing composer and the `runner/datasets/` / `runner/execution/` split describe pending development work. Commands below using these new paths require that development checkout. See [CHANGELOG](CHANGELOG) for commit-based history and [Read the Docs](https://shybox.readthedocs.io/en/latest/) for the published guide.

## Features

| Stage | Capabilities |
| --- | --- |
| Prepare data | IFS/ICON and observed-data conversion; spatial interpolation, masking, resampling, point-to-time-series conversion and grid-to-section averaging. |
| Derive hydrological inputs | Soil-moisture and evapotranspiration merging, watershed delineation, discharge return times and RainFARM rainfall disaggregation. |
| Compose model forcing | Combine observed/forecast NetCDF, GeoTIFF, raw binary and ESRI ASCII sources into HMC/S3M NetCDF forcing, including gzip inputs and outputs. |
| Plan and check coverage | `DatasetPlanner` provides validity/source-cycle tables, per-variable/member paths, bounded availability checks, trailing observed-gap forecast fallback and ensemble file dictionaries. |
| Transform and validate | Explicit cube splitting and nearest/linear spatial interpolation; prepared-file copying; terrain, time, units, dimensions and gap-policy checks before complete-run publication. |
| Prepare ensembles | Member-specific rainfall with shared deterministic fields, configurable member identifiers, sequential composition and manifests for the complete ensemble. |
| Execute models | HMC/S3M namelist preparation and executable management; HMC sequential/parallel ensemble scheduling, retries and member-specific paths. |
| Analyze results | Simulated/observed HMC time series; NetCDF output and Continuum hydrograph JSON, either per section or in a collection. |
| View and publish | Hydrapp Flask viewer with legacy HAT PNG/JSON maps/charts, date/source filters, interactive NetCDF section maps and deterministic/ensemble hydrographs. |
| Monitor runs | Start/heartbeat/end records, exit status, retries, ensemble completeness, RAM history and integrated Ruler process/host reports. |
| Reuse components | JSON/LUT configuration, environment and time placeholders, process chains and pickle/NetCDF/Zarr temporary storage. |

## Install the scientific environment

From a Linux shell:

```bash
git clone https://github.com/c-hydro/shybox.git
cd shybox
bash setup_shybox_base.sh
source ./conda/shybox_base_settings
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
```

The scientific environments use **Python 3.12**. Geographic preparation can use `bash setup_shybox_geo.sh` followed by `source ./conda/shybox_geo_settings`. Dependencies are listed in [requirements_shybox_base.yaml](requirements_shybox_base.yaml) and [requirements_shybox_geo.yaml](requirements_shybox_geo.yaml).

Supply operational inputs from [SHYDATA](https://github.com/c-hydro/shydata) or your own sources. Native HMC/S3M runs also require separately compiled, compatible model executables, geographic grids and any selected restart data. RFarm and PRISM repositories are declared in [pyproject.toml](pyproject.toml) and managed by `shybox/external_toolkit/`.

## Plan datasets before model execution

`DatasetPlanner` is available on `main` in `shybox.runner_toolkit.datasets.planning_handler`. It builds observed/forecast time tables and per-variable/member file plans, checks a selected availability window, and fills a trailing observed gap from complete forecast sources. It plans filenames and checks file presence without opening arrays or launching a model.

```python
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner

# application contains resolved info, time, data_source and data_destination sections.
planner = DatasetPlanner.from_application(application)
planner.update_time(
    n_step_to_check_obs=3, n_step_to_check_frc=1,
    flag_adjust_obs_frc=True,
    rules_to_fill_obs="fill_using_frc", rules_to_fill_frc=None,
)
files = planner.file_plan
available = planner.file_available()
missing = planner.file_not_available()
summary = planner.file_table("source")
by_member = planner.file_by_ensemble()
```

This checks only signed steps **-3 through +1**. Both limits default to `None`, disabling checks. Unchecked availability remains unknown; fallback requires all required forecast files and members. Repeated calls can restore observations that arrive later. Planner modes and `{time_mode}` filenames use `observed`/`forecast`.

See the [dataset planning guide](docs/dataset_planning.rst) for a complete configuration, source `dims`, deferred time tags, ensemble aliases and the `{member: {time: {variable: filename}}}` result. The [runner guide](docs/runners.rst#dataset-planning-and-model-execution) explains how dataset planning relates to namelist and executable configuration. Prepare the actual model forcing files before execution; planning does not compose them.

## Configure dataset workflows

The dataset entry point under `workflow/runner/datasets/` configures `DataLocal` source/destination pairs and variable workflows in `OrchestratorConfig`, following the NWP converter pattern. Model launch is separate. The shared dataset driver supports these profiles:

| Profile | Example in `workflow/runner/datasets/` |
| --- | --- |
| HMC deterministic | `app_runner_workflow_hmc_datasets_forcing_deterministic.json` |
| HMC rainfall ensemble | `app_runner_workflow_hmc_datasets_forcing_probabilistic.json` |
| S3M deterministic | `app_runner_workflow_s3m_datasets_forcing.json` |

Copy a profile and adapt its terrain, variable names, units and input filename conventions. For example, from the repository root with the scientific environment active:

```bash
mkdir -p exec/my_forcing
cp workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
  exec/my_forcing/settings.json

export DOMAIN_NAME="my_domain"
export PATH_GEO="/absolute/path/to/geographic/grids"
export PATH_OBS="/absolute/path/to/observations"
export PATH_FRC_NWP="/absolute/path/to/weather/forecasts"
export PATH_DST="$PWD/exec/my_forcing/forcing"
export PATH_LOG="$PWD/exec/my_forcing/log"

python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
  -settings_file "$PWD/exec/my_forcing/settings.json" \
  -time "2026-11-09 09:35" --dry-run --plan-files
```

The command plans paths, checks signed steps **-3 through +1**, and configures one orchestrator per ensemble member and validity time. It writes logs and prints a JSON report with `--dry-run`; `--plan-files` includes file rows. Setup does not read arrays or write forcing outputs.

Each source maps input fields to canonical variables and each destination collection maps canonical variables to model field names. Sources declare `dims: "2d"` or `"3d"`. The top-level `workflow.process_list` configures `RAIN_WORKFLOW`, `AIR_T_WORKFLOW`, `INC_RAD_WORKFLOW`, `REL_HUM_WORKFLOW` and `WIND_SPEED_WORKFLOW`. All variable workflows for an output step share one orchestrator, keeping fields for a combined forcing file together.

The sample process lists pass data through; configure interpolation, masking and unit conversion explicitly. For probabilistic HMC, select its profile and set `PATH_FRC_RFARM`; its destination template declares a `run_{ensemble_member}` folder. Python callers can use `configure_dataset_workflows(planner, workflow_configuration)` from the dataset entry point and explicitly run the returned orchestrators when the inputs are ready.

Read the [dataset planning and workflow guide](docs/dataset_planning.rst) for complete configuration, time references, bounded checks, fallback, ensemble grouping and workflow setup. The [forcing-composer guide](docs/datasets.rst) describes the separate development composition design, including its validation and publication policies.

## Execute models and analyze results

Follow the [HMC/S3M runner guide](docs/runners.rst) to configure model versions, namelists, executable/library paths, forcing intervals and restart times. Drivers and examples are in `workflow/runner/execution/`. HMC ensemble execution supports sequential/parallel scheduling and retries; isolate working directories, namelists, logs, forcing and state paths per member.

For deterministic HMC time-series analysis:

```bash
python workflow/analyzer/app_analyzer_workflow_hmc_time_series_main.py \
  -settings_file workflow/analyzer/app_analyzer_workflow_hmc_time_series_destine_marche_deterministic_netcdf.json \
  -time "2026-08-11 12:00"
```

Adapt the example's paths and domain configuration first. The analyzer joins simulated and observed data with a section registry. Destination configuration selects NetCDF, JSON per section (`layout: "sections"`), or one JSON array for all sections (`layout: "collections"`). JSON output preserves the Continuum comma-separated string schema, supports metadata/variable mappings and replaces files atomically. See [time-series output configuration](docs/workflows.rst#time-series-json-output) and the [workflow guide](docs/workflows.rst).

Probabilistic aggregation in the analyzer remains under development. Hydrapp can display an existing NetCDF ensemble dimension; it does not combine separate member files or read hydrograph JSON exports.

## View results with Hydrapp

```bash
bash setup_shybox_hydrapp.sh
hydrapp/.venv/bin/python -m hydrapp.app --config hydrapp/config.demo.json
```

Open **http://127.0.0.1:5050** for published figures, `/maps` for NetCDF section-map previews, and `/monitoring` for execution/RAM status. The dedicated setup includes Flask, psutil and Matplotlib. The viewer lives in `hydrapp/`, replacing the earlier `dashboard/` path.

Configure real runs in `hydrapp/config.json`, including mode, NetCDF location, filename convention and ensemble dimension. Restart after configuration changes; **Refresh runs** discovers new files for loaded configurations. The portable demo includes 23 legacy HAT PNG/JSON figure products. See [Hydrapp setup and data contract](docs/hydrapp.rst).

Published figures provide images and metadata. Numerical raster overlays, pixel inspection and production map generation remain future work. Interactive chart/map libraries and map tiles require internet access; published figures and monitoring use local assets.

## Monitor execution and resources

Run these examples from the repository root in an environment with the monitoring dependencies:

```bash
# Explicit lifecycle instrumentation around application code
python -m workflow.monitoring.app_monitoring_main_example

# Run-status demo with real RAM/Ruler snapshots for the Hydrapp demo
python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources

# Monitor a real process; replace 12345 with its PID
python -m workflow.monitoring.app_monitoring_memory_example \
  --config workflow/monitoring/ruler_monitoring.example.json --pid 12345 --samples 6 --plot
```

Lifecycle records capture start, heartbeat, end and exit status. Resource collectors select processes by PID/name/command, optionally include descendants, and write memory histories and Ruler reports. Configure lifecycle and resource output folders independently. A stale heartbeat or available output file alone does not establish successful execution.

See the [monitoring guide](docs/monitoring.rst), [Hydrapp guide](docs/hydrapp.rst) and [monitoring examples](docs/monitoring.rst#process-ram-and-ruler). The scientific environment may need `psutil` installed separately; Matplotlib is required for RAM figures.

## Repository layout

```text
shybox/                    Reusable configuration, I/O, processing and runner toolkits
workflow/
  dataset/                 Upstream conversion, merging and rainfall disaggregation
  geo/                     Geographic preparation and catchment delineation
  runner/datasets/         Observed/forecast forcing planning and composition
  runner/execution/        HMC/S3M execution and HMC ensemble examples
  analyzer/                Model-output time-series analysis
  monitoring/              Lifecycle, RAM, Ruler and run-status examples
hydrapp/                   Flask viewer, portable demos and tests
apps/                      Additional applications and prototypes
web/                       Earlier visualization application
utils/                     Standalone utilities, including HMC state copying
docker/, apptainer/, hpc/  Deployment and execution material
tests/                     Automated checks, examples and fixtures
docs/                      Sphinx / Read the Docs sources
setup_shybox_*.sh          Scientific, geographic and Hydrapp environment setup
```

Local `conda/`, `dset/`, `exec/` and `tmp/` directories hold environments, inputs, results and ancillary files and are ignored by Git.

## Checks and documentation

Run dataset checks with the scientific environment active:

```bash
python -B -m unittest discover -s tests/dataset -p 'test_*.py' -v
```

The broader suites cover source formats, time/file planning, forcing composition, HMC/S3M namelists, scheduling/retries, monitoring, geography, JSON I/O and Flask routes. Synthetic tests do not replace native model runs or validate forecast skill. RainFARM integration and operational datasets need their own compatible environment.

- [Read the Docs](https://shybox.readthedocs.io/en/latest/): published user and development guides.
- [Dataset preparation](docs/datasets.rst): forcing configuration, planning, validation and ensembles.
- [HMC/S3M execution](docs/runners.rst), [workflow/output guide](docs/workflows.rst), [monitoring](docs/monitoring.rst) and [Hydrapp](docs/hydrapp.rst).
- [Build and test instructions](docs/development.rst), [publication instructions](docs/publishing.rst) and [CHANGELOG](CHANGELOG).

Documentation edition **0.3.0** adds the dataset guide and is numbered independently of the package. Documentation is published from `main`; Read the Docs `latest` updates after a successful remote build. Documentation dependencies are pinned in [docs/requirements.txt](docs/requirements.txt).

## License

SHYBOX uses the [European Union Public License v1.2 (EUPL-1.2)](LICENSE). The integrated Ruler engine retains its [MIT licence and provenance](https://github.com/c-hydro/shybox/blob/44922b3dd1452176803759db5e3913a159689e2b/shybox/monitoring_toolkit/resources/ruler/SOURCE.md).
