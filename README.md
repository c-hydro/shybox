# SHYBOX — Snow and Hydrologic Modeling Toolbox

[![License](https://img.shields.io/badge/license-EUPL--1.2-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/environment-Python%203.12-blue)](requirements_shybox_base.yaml)
[![Documentation Status](https://readthedocs.org/projects/shybox/badge/?version=latest)](https://shybox.readthedocs.io/en/latest/)

**SHYBOX** (Snow and HYdrologic toolBOX) provides Python tools and JSON-configured workflows for preparing environmental datasets, running **HMC / Continuum** and **S3M**, and analyzing model results. It is developed by the **CIMA Research Foundation** for hydrological and snow-modeling applications.

The current package metadata identifies version **1.6.0**. This README describes the development checkout; see [CHANGELOG](CHANGELOG) for dated updates and commit references.

## Current capabilities

| Area | Available functionality |
| --- | --- |
| Configuration and time | Command-line arguments, JSON settings, environment-variable and time placeholders, and shared logging. |
| Processing workflows | Configurable process chains connecting local, in-memory and on-demand datasets. |
| Grid and point processing | Weather-forcing conversion, spatial interpolation, masking, resampling, grid-to-section averaging and point-to-time-series conversion. |
| Hydrological processing | Soil-moisture and evapotranspiration merging, discharge return-time calculations, and catchment delineation from section coordinates. |
| Model execution | HMC and S3M namelist preparation and executable management; HMC deterministic and ensemble orchestration with sequential/parallel execution and per-member settings. |
| RainFARM integration | NWP rainfall disaggregation, ensemble member handling and ancillary files. The adapter and native-output integration are under development. |
| Temporary storage | Generic `DataTemporary` storage using pickle, or NetCDF/Zarr for xarray objects, with configurable folders, filenames and prefixes. |
| Dataset I/O | Format-specific NetCDF, GRIB, GeoTIFF and CSV/ASCII handlers; HMC time-series export to NetCDF or Continuum hydrograph JSON. |
| Hydrapp | Flask viewer in `hydrapp/`: HAT published maps/charts, date/window/source filters, interactive NetCDF section maps and deterministic/ensemble hydrographs. |
| Publishing catalogue | Read-only legacy HAT PNG/JSON discovery, normalized metadata, image/metadata downloads and 23 portable demo products. |
| Execution monitoring | Atomic start/heartbeat/end records, exit codes, retries, ensemble completeness, expected-run checks and a Hydrapp monitoring page. |
| RAM and Ruler | JSON-configured PID/name/command selectors, optional descendants, RSS/VMS/peak history, daily CSV/PNG, host memory and Ruler disk/load/process reports, native workspaces/CSV and report figures. |

### Time-series output

The HMC analyzer joins simulated and observed series with a section registry. Its destination configuration selects NetCDF or JSON output:

- **NetCDF:** one file containing time, sections, variables and section metadata.
- **JSON per section:** `layout: "sections"` with a filename such as `hydrograph_{section_tag}.json`.
- **JSON collection:** `layout: "collections"` with one JSON array containing all section objects.

The JSON writer preserves the Continuum comma-separated string format, supports configurable run/section/variable mappings, and writes one section at a time. It checks filename collisions and replaces output files atomically. See [time-series JSON configuration](docs/workflows.rst#time-series-json-output).

### Current integration limits

The time-series analyzer currently processes deterministic series. Aggregating probabilistic HMC member outputs into one ensemble time-series dataset remains to be implemented. The dashboard can read an existing NetCDF with an ensemble dimension; it does not combine separate member files or read the new hydrograph JSON exports yet.

Hydrapp displays published raster figures and interactive section locations. Interactive raster overlays/pixel values and production map aggregation/rendering remain future work. The legacy PNG/JSON archive does not supply numerical series or raster values. Interactive map tiles and chart/map libraries require internet access; published figures and monitoring use local assets.

## Repository structure

```text
shybox/
├── shybox/                    # Reusable Python package and processing toolkits
├── workflow/
│   ├── dataset/               # Conversion, merging and rainfall disaggregation
│   ├── geo/                   # Geographic preparation and catchments
│   ├── runner/                # HMC/S3M execution and HMC ensemble examples
│   ├── analyzer/              # HMC time-series analysis
│   └── monitoring/            # Lifecycle, RAM, Ruler and run-status demos
├── hydrapp/                   # Hydrapp viewer, portable demos, configuration and tests
├── web/                       # Earlier, unfinished visualization application
├── apps/                      # Additional applications and prototypes
├── docker/                    # Container definitions and entrypoints
├── apptainer/                 # Apptainer deployment notes
├── hpc/                       # HPC execution material
├── utils/                     # Standalone utilities
├── docs/                      # Sphinx / Read the Docs sources
├── tests/                     # Tests, examples and fixtures
├── pyproject.toml             # Package metadata and external-toolkit declarations
├── setup_shybox_base.sh       # Base scientific environment
├── setup_shybox_geo.sh        # Geospatial environment
└── setup_shybox_hydrapp.sh      # Independent dashboard environment
```

Local checkouts may also contain `conda/`, `dset/`, `exec/` and `tmp/` for environments, input datasets, execution results and ancillary files. These directories are ignored by Git.

## Installation

The supplied scientific environments use **Python 3.12**. From a Linux shell:

```bash
git clone https://github.com/c-hydro/shybox.git
cd shybox
bash setup_shybox_base.sh
source ./conda/shybox_base_settings
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
```

For geographic preparation requiring the additional GIS dependencies, use `bash setup_shybox_geo.sh` and source `./conda/shybox_geo_settings`. The corresponding dependency lists are [requirements_shybox_base.yaml](requirements_shybox_base.yaml) and [requirements_shybox_geo.yaml](requirements_shybox_geo.yaml).

HMC and S3M require separately prepared model executables and compatible inputs. RainFARM workflows require the external RFarm package. External-toolkit discovery and installation are managed under `shybox/external_toolkit/`, with RFarm and PRISM repositories declared in `pyproject.toml`.

## Running a workflow

Choose a driver and its JSON example under `workflow/`. Set the input/output paths, domain, model settings and environment variables referenced by that configuration before execution. For example:

```bash
python workflow/analyzer/app_analyzer_workflow_hmc_time_series_main.py \
  -settings_file workflow/analyzer/app_analyzer_workflow_hmc_time_series_destine_marche_netcdf.json \
  -time "2026-08-11 12:00"
```

Workflow examples include development-machine paths and domain-specific settings; adapt them to your data. Obtain inputs from [SHYDATA](https://github.com/c-hydro/shydata) or your own operational sources. Model binaries and operational datasets are supplied separately.

## Running Hydrapp

From the repository root:

```bash
bash setup_shybox_hydrapp.sh
hydrapp/.venv/bin/python -m hydrapp.app --config hydrapp/config.demo.json
```

Open **http://127.0.0.1:5050** for published figures, `/maps` for map previews and `/monitoring` for execution/RAM status. The old `dashboard/` application path is now `hydrapp/`. The setup script creates a dedicated Flask and monitoring environment.

Declare configurations under `runs` in [hydrapp/config.json](hydrapp/config.json), including deterministic/probabilistic mode, NetCDF location, filename convention and ensemble dimension where applicable. Restart after changing the configuration; **Refresh runs** discovers new result files for loaded configurations. See [dashboard setup and data contract](hydrapp/README.md).

## Monitoring examples

Run with the SHYBOX scientific environment, from the repository root:

```bash
# Explicit monitor.start()/finish() around application code
python -m workflow.monitoring.app_monitoring_main_example

# Live status demo for hydrapp/config.demo.json, with real RAM/Ruler snapshots
python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources

# Sample a specific process using a JSON configuration; replace 12345 with its PID
python -m workflow.monitoring.app_monitoring_memory_example \
  --config workflow/monitoring/ruler_monitoring.example.json --pid 12345 --samples 6 --plot
```

Hydrapp setup includes `psutil` and Matplotlib for the monitoring demos. Install
`psutil` separately when using a different collector environment (`python -m pip install psutil`).
Matplotlib is required for RAM figures. See [monitoring configuration](docs/monitoring.rst),
[Hydrapp usage](docs/hydrapp.rst) and [runnable examples](workflow/monitoring/README.md).
Lifecycle and resource output folders are configured independently. A stopped
heartbeat is stale; output availability does not prove process success.

## Verification

The test suites cover HMC 3.1.6/3.2.0/3.3.0 and S3M 5.3.3 namelists,
member scheduling/retries, execution status, resource/Ruler collection, coordinate
alignment/catchment area, published assets and Flask routes. Tests use synthetic
inputs and subprocesses; native HMC/S3M binaries and operational data are not bundled.
See [test commands and limitations](docs/development.rst#automated-checks).

## Documentation

- [Read the Docs](https://shybox.readthedocs.io/en/latest/): installation, configuration and workflow guidance.
- [HMC and S3M runner guide](docs/runners.rst): domain setup, namelists, dry runs and execution.
- [Documentation sources](docs/index.rst), [development/build instructions](docs/development.rst) and [publishing instructions](docs/publishing.rst).
- [CHANGELOG](CHANGELOG): package development and release history.

The documentation edition **0.2.0** is numbered independently of the package. Its Sphinx build uses the dependencies in [docs/requirements.txt](docs/requirements.txt).

## License

SHYBOX is licensed under the [European Union Public License v1.2 (EUPL-1.2)](LICENSE).

The integrated Ruler engine retains its [MIT licence and provenance](shybox/monitoring_toolkit/resources/ruler/SOURCE.md).
