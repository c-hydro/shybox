Preparing forcing datasets
==========================

The forcing composer combines observed and forecast datasets into HMC/Continuum
or S3M NetCDF forcing. It plans validity times and source cycles, checks file
availability, reads each variable, validates it against the terrain grid and
publishes a complete run with provenance. Deterministic fields can be shared
across rainfall ensemble members.

This guide describes the development composition design. The published planner
and dataset workflow setup are documented in :doc:`dataset_planning`; their
entry point configures ``DataLocal`` / ``OrchestratorConfig`` without publishing
forcing files. The development composition interfaces below are separate. The new
``workflow/runner/datasets/`` and ``shybox/runner_toolkit/datasets/`` interfaces
must be present in your revision. Preparing this documentation does not publish
the associated application code. Model execution is covered in :doc:`runners`.

Choose the preparation stage
----------------------------

.. list-table:: Dataset workflow responsibilities
   :header-rows: 1
   :widths: 36 64

   * - Directory
     - Purpose
   * - ``workflow/dataset/convert/``
     - Convert upstream weather/station products, interpolate grids and derive time series.
   * - ``workflow/dataset/merge/``
     - Merge tiles, time steps, soil moisture or evapotranspiration over a reference domain.
   * - ``workflow/dataset/disaggregate/``
     - Generate rainfall ensembles with the external RainFARM integration.
   * - ``workflow/runner/datasets/``
     - Assemble prepared observed/forecast variables into validated model forcing.
   * - ``workflow/runner/execution/``
     - Prepare namelists and launch HMC/S3M using the forcing files.

Use upstream conversion when inputs need GRIB decoding, reprojection,
accumulation differencing or scientific derivation. The composer accepts NetCDF,
GeoTIFF, raw binary and ESRI ASCII grids, including gzip-compressed files. It
can select exact times from cubes and interpolate spatially when configured.
It does not download inputs, generate rainfall ensembles or launch a model.

Plan your first run
-------------------

From the repository root, activate the scientific environment as described in
:doc:`installation`. A model executable is unnecessary for forcing preparation.
Copy one of these profiles before changing domain paths, units or source names:

.. list-table:: Profiles in ``workflow/runner/datasets/``
   :header-rows: 1
   :widths: 20 80

   * - Profile
     - JSON filename
   * - HMC deterministic
     - ``app_runner_workflow_hmc_datasets_forcing_deterministic.json``
   * - HMC probabilistic
     - ``app_runner_workflow_hmc_datasets_forcing_probabilistic.json``
   * - S3M deterministic
     - ``app_runner_workflow_s3m_datasets_forcing.json``

All three use ``app_runner_workflow_hmc_datasets_main.py``. Despite its name,
this shared driver selects HMC or S3M from the configuration. The HMC examples
use hourly observed grids, an ECMWF forecast cube, and RainFARM rainfall members
for the probabilistic profile. S3M's example uses prepared files at each time.

.. code-block:: bash

   source ./conda/shybox_base_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
   mkdir -p exec/my_forcing
   cp workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
      exec/my_forcing/settings.json

   export DOMAIN_NAME="my_domain"
   export PATH_GEO="/absolute/path/to/geographic/grids"
   export PATH_OBS="/absolute/path/to/observations"
   export PATH_FRC_NWP="/absolute/path/to/weather/forecasts"
   export PATH_FRC_RFARM="/absolute/path/to/rainfall/members"
   export PATH_DST="$PWD/exec/my_forcing/forcing"
   export PATH_LOG="$PWD/exec/my_forcing/log"

   python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
      -settings_file "$PWD/exec/my_forcing/settings.json" \
      -time "2026-11-09 09:35" --dry-run --plan-files

Replace the absolute input paths and adapt the copied JSON to actual filenames,
coordinate names and domain terrain. ``PATH_FRC_RFARM`` is needed only by the
probabilistic HMC profile. The default terrain name is ``{domain_name}.dem.txt``.
The planning command can run without the input files: missing files are reported
rather than read. It creates logs, but no forcing output directory or data arrays.

``--dry-run`` prints a JSON summary after the configuration/log tables;
``--plan-files`` includes individual file rows in that summary. Inspect the
rounded run, selected periods, source issue time, destination paths and member
IDs before composition. The current driver calls
``update_time(n_steps_observed=4, n_steps_forecast=0)``: its file checks cover
signed steps -4 through zero only. Future file counts are planned totals, and
unchecked availability remains null. This is not a full-window validation.

Remove ``--dry-run --plan-files`` from the command to read and validate the
complete selected period and publish forcing. Existing run folders are rejected;
choose a new destination root when repeating an experiment. Once successful,
configure the model's forcing paths and interval using :doc:`runners`.

Configuration layout and variable mapping
-----------------------------------------

The profiles use the standard SHYBOX ``configuration`` root. Environment LUT
values supply roots and domain names; user LUT values supply time windows,
ensemble ranges and deferred tags. ``format`` declares their types and
``template`` declares date/member formatting. See :doc:`configuration`.

.. list-table:: Sections under ``configuration.application``
   :header-rows: 1
   :widths: 30 70

   * - Section
     - Responsibility
   * - ``info``
     - ``domain_name`` and ``model`` (``hmc`` or ``s3m``).
   * - ``time``
     - Run, frequency, rounding, observed/forecast windows and cycle selection.
   * - ``geo.terrain``
     - Terrain path, filename and format used to validate the forcing grid.
   * - ``data_source.observed`` / ``data_source.forecast``
     - One entry per input variable, including operations and gap policy.
   * - ``data_destination``
     - Run folder, filename, model writer, variable names and units.
   * - ``log``
     - Log folder and filename, including logs from planning calls.

Each source's ``variable`` maps exactly one case-sensitive input field name to a
canonical key. ``data_destination.variables`` maps that canonical key to the
model's output field name. For example, source ``{"RAIN": "rain"}`` and
destination ``{"rain": "Rain"}`` produce ``RAIN → rain → Rain``. Declare separate
entries even when several variables come from the same file. Duplicate canonical
mappings and unknown keys are rejected. There is no ``application.variables`` block.

This complete source-entry fragment belongs under an observed or forecast group:

.. code-block:: json

   {
     "rain": {
       "path": "{path_forecast_nwp}/{path_time_forecast_nwp}",
       "file_name": "rain.{file_time_forecast_nwp}.nc",
       "format": "netcdf",
       "x": "longitude",
       "y": "latitude",
       "time": "time",
       "nodata": -9999,
       "variable": {"RAIN": "rain"},
       "operations": {"interpolate": null, "split": {"method": "3d_to_2d"}},
       "mode": "deterministic",
       "ensemble": null,
       "required": true,
       "tolerance_steps": 0,
       "missing_policy": "error",
       "as_is": false
     }
   }

Rainfall, air temperature, incoming radiation, relative humidity and wind speed
are mandatory canonical variables: ``rain``, ``air_t``, ``inc_rad``, ``rel_hum``
and ``wind_speed``. Additional variables can be configured. Output units are
provided in ``data_destination.units``; source ``scale`` and ``offset`` explicitly
configure numerical conversion when necessary. Units otherwise must match.
Untagged input units are assumed to match the configured units.

This HMC destination fragment combines the fields in one file per validity time:

.. code-block:: json

   {
     "path": "{path_destination}/{path_time_run}",
     "file_name": "{time_mode}/{path_time_destination}/hmc.forcing-grid.{file_time_destination}.nc.gz",
     "format": "netcdf",
     "type": "hmc",
     "variables": {
       "rain": "Rain", "air_t": "Air_Temperature",
       "inc_rad": "Incoming_Radiation", "rel_hum": "Relative_Humidity",
       "wind_speed": "Wind"
     },
     "units": {
       "rain": "mm", "air_t": "C", "inc_rad": "W m-2",
       "rel_hum": "%", "wind_speed": "m s-1"
     }
   }

Use the S3M profile's writer settings for snow-model output. Destination format
is NetCDF with ``.nc`` or ``.nc.gz`` filenames. ``{time_mode}`` becomes ``obs`` or
``frc`` from the selected source group; date tags use validity time. Source
operations, modes and gap policies belong on source entries.

Validity times and forecast cycles
----------------------------------

The composer uses naive UTC timestamps. CLI ``-time`` overrides the configured
run, which is rounded according to ``time_rounding``. ``time_frequency`` sets the
forcing cadence. ``hour_reference_observed`` and ``hour_reference_forecast`` select
issue clocks; null uses the rounded run. A clock later than that run selects the
previous day's cycle, so the composer never selects a future issue.

These are user LUT values; ``application.time`` references them and maps
``time_priority_*`` to ``priority_*``:

.. code-block:: json

   {
     "time_run": "2026-11-09 09:35",
     "time_rounding": "1h",
     "time_frequency": "1h",
     "hour_reference_observed": null,
     "hour_reference_forecast": "00:00",
     "time_look_observed": "48h",
     "time_horizon_observed": "midnight",
     "time_look_forecast": "72h",
     "time_horizon_forecast": "midnight",
     "time_priority_observed": "min",
     "time_priority_forecast": "min"
   }

For each side, the planner compares a look boundary from the rounded run with a
horizon boundary from its source reference. The horizon candidate subtracts or
adds the look duration, then floors to the configured horizon. ``midnight`` means
a day boundary; null disables the horizon candidate. ``min``/``max`` select the
earlier/later boundary timestamp; ``look``/``horizon`` select the candidate directly.
Observed priority compares start times; forecast priority compares end times.
The selected start ceils and the selected end floors to the forcing cadence.

The example rounds to 9 November at 09:00, with forecast issue at 00:00.
``min/min`` yields 58 observed steps from 7 November 00:00 through 9 November
09:00 and 63 forecast steps from 9 November 10:00 through 12 November 00:00.
The period has 121 unique validity times. ``max/max`` instead selects 49 observed
and 72 forecast steps. Endpoints are inclusive; forecast starts one step after
the observed cutoff.

Step files use validity-time references. Cube sources with
``split.method: "3d_to_2d"`` use a fixed source cycle across all validity dates.
``reference_mode: "step"`` or ``"3d"`` can override this inference per source.
A cube's source date must remain its issue date when validity crosses midnight.

.. list-table:: Deferred path and file tags
   :header-rows: 1
   :widths: 48 52

   * - Tags
     - Reference
   * - ``path_time_observed``, ``file_time_observed``
     - Observed source step or cycle.
   * - ``path_time_forecast_nwp``, ``file_time_forecast_nwp``
     - Weather forecast source step or cycle.
   * - ``path_time_forecast_rfarm``, ``file_time_forecast_rfarm``
     - RainFARM source step or cycle.
   * - ``path_time_run``
     - Rounded run time, usually formatted as ``%Y%m%d%H%M``.
   * - ``path_time_destination``, ``file_time_destination``
     - Output validity time, usually ``%Y/%m/%d`` and ``%Y%m%d%H%M``.

Declare deferred date tags with null LUT values, ``format: "time"`` and a strftime
``template``. The ``*_forecast_nwp_valid`` and ``*_forecast_rfarm_valid`` tags
explicitly use validity time for already separated forecast files.

For the published metadata-only planner, use :doc:`dataset_planning`.
It documents ``DatasetPlanner`` independently of the development composer
below, including its grouped destinations and ``observed``/``forecast`` modes.

Inspect and refresh a plan with DatasetManager
----------------------------------------------

``DatasetManager`` owns time/mode configuration, availability and composition.
The CLI uses ``ConfigManager`` to resolve its LUT before constructing the manager.
For Python integration, start with a resolved application dictionary, preserving
deferred date tags until a source cycle and validity time are known:

.. code-block:: python

   from shybox.runner_toolkit.datasets.datasets_handler import DatasetManager

   # resolved_application comes from ConfigManager; date_tags holds LUT formats.
   datasets = DatasetManager.from_application(
       resolved_application, time_tags=date_tags, time_run="2026-11-09 09:35"
   ).configure()
   period = datasets.time_period
   files = datasets.plan_files(check_exists=True)
   datasets.update_time(n_steps_observed=48, n_steps_forecast=72)
   summary = datasets.as_dict(include_files=True)
   # Requires terrain and complete, valid input coverage:
   output_folder = datasets.run()

``from_settings`` accepts the normalized composer dictionary instead. Geography
is optional for planning and required for ``run()``. Configuration copies settings;
``time_table`` and ``file_plan`` return copies of their stored tables.

``plan_files(check_exists=True)`` checks regular-file presence across the plan.
Without existence checks, availability flags are null. Call it again to refresh
checks after files arrive. It does not inspect time coverage inside a cube,
units, dimensions, values or grid compatibility.

The time table exposes sequential ``step`` numbers starting at one, signed
``time_step`` offsets with zero at the observed cutoff, ``time_mode``, source
references and ``file_id``. File rows are ordered by step and member and identify
the variable, source/output member and source/destination paths. Actual paths
receive ``source_file_id`` and ``destination_file_id``; shared files reuse an ID.
An unconfigured source has ID zero and no source path. Per-time file counts and
summary totals count unique paths rather than variable rows.

``update_time`` checks inclusive signed windows around zero. ``None`` disables
one side; zero checks the cutoff only; both arguments omitted disable all checks.
``time_checked`` marks selected times and ``time_available`` reports required-file
presence, with null outside the window. Unchecked file flags also remain null.

Within this window, a missing required observation can switch the entire step
to forecast at the same validity time, provided every required forecast file
exists for every output member. Missing optional fields do not trigger fallback.
If both source groups are incomplete, the observed step remains unavailable.
Repeated calls can restore observations that arrive later. This is file-presence
fallback; execution still validates the selected contents. Unavailable steps
remain in the plan and are subject to execution's gap policy.

Spatial operations, formats and prepared files
-----------------------------------------------

``operations.interpolate`` and ``operations.split`` are null when disabled.
Enable only the operations needed by that source:

.. code-block:: json

   {
     "interpolate": {"method": "nn", "roi": 18000},
     "split": {"method": "3d_to_2d"}
   }

Interpolation supports nearest (``nn``/``nearest``) and ``linear``. ROI is a
positive spherical distance in metres; null removes the distance cap. Targets
outside source extent remain missing. Nearest interpolation does not conserve
rainfall totals. Reversed axes can be reordered without interpolation; different
grids require explicit interpolation. Reprojection belongs upstream.

``3d_to_2d`` selects the exact requested time from a NetCDF cube; multi-time
sources require it. Without splitting, 2D and singleton-time inputs retain their
dimensions. Extra vertical/ensemble dimensions are rejected. There is no nearest
time selection, temporal interpolation or accumulation differencing.

.. list-table:: Format-specific configuration
   :header-rows: 1
   :widths: 20 80

   * - Format
     - Input contract
   * - ``netcdf``
     - Configure ``x``, ``y`` and ``time`` names. Coordinates may be 1D axes or matching 2D grids.
   * - ``tiff``
     - Configure ``band`` explicitly for multiband rasters.
   * - ``ascii``
     - Georeferenced ESRI ASCII raster, rather than a station CSV or arbitrary text matrix.
   * - ``binary``
     - Declare ``binary`` dtype/order/header bytes and ``grid`` coordinates or a terrain reference.

For example, raw binary settings can use
``"binary": {"dtype": "<i4", "order": "F", "header_bytes": 0}`` and
``"grid": {"reference": "terrain"}``. C and Fortran array orders are supported;
byte counts must match. Record-marked Fortran files and binary cubes need upstream
conversion. Raster grids must be geographic EPSG:4326; absent raster CRS is
assumed geographic. Terrain accepts ASCII/TIFF/NetCDF and requires regular square
cells; terrain nodata identifies inactive cells.

Set ``as_is: true`` only for complete model-ready NetCDF files. Every destination
variable must come from the same source file and use that flag. Validation checks
names, units, time, grid, dimensions, floating dtype and values before copying the
NetCDF bytes; only the gzip envelope may change. Splitting, interpolation,
renaming and nonidentity scaling are incompatible with this mode. The HMC
observed profile uses it; forecast cubes use explicit splitting and ``as_is: false``.

Ensembles, gaps and publication
-------------------------------

A deterministic source uses ``mode: "deterministic"`` and ``ensemble: null``.
Probabilistic sources declare an inclusive member range and member format:

.. code-block:: json

   {
     "mode": "probabilistic",
     "ensemble": {"start": 1, "end": 10, "format": "03d"}
   }

Their source paths or filenames must include ``{ensemble_member}``; deterministic
sources must not. All probabilistic sources share ordered member IDs. The HMC
probabilistic profile takes RainFARM rain members 001–010 and shares observations
and other weather fields. Members are composed sequentially. The 121-step example
plans 1,210 output files, with a complete forcing period for each member.

Mandatory fields are required at the first timestamp of each source period.
For later gaps, ``tolerance_steps`` bounds consecutive missing steps and
``missing_policy`` selects ``error``, ``persist`` or ``nodata``. Defaults are zero
tolerance and ``error``. Persistence uses the previous available field within
that source group/member. Missing optional variables are allowed. Missing active
cells make the whole field missing; corrupt data, invalid dimensions, grid
mismatches and incorrect units fail validation immediately.

Outputs are staged, reopened and checked for time, dimensions, coordinates,
floating dtype, units, missing values and numerical round trip. The complete
run or ensemble is renamed into its final location only after success. Existing
runs are never overwritten. Each run/member has ``manifest.json`` with source
paths, operation settings, availability/persistence provenance and mode/member
labels; ensemble roots also record their member folders.

Typical output layout with the supplied HMC templates:

.. code-block:: text

   forcing/202611090900/
     manifest.json
     obs/2026/11/07/hmc.forcing-grid.202611070000.nc.gz
     frc/2026/11/09/hmc.forcing-grid.202611091000.nc.gz

   # Probabilistic output adds a member directory below the run root:
   forcing/202611090900/run_001/manifest.json
   forcing/202611090900/run_001/obs/2026/11/07/...
   forcing/202611090900/run_001/frc/2026/11/09/...

Compressed input cubes are decompressed once per member and NetCDF reads remain
lazy. Field processing retains one timestamp and previous values per variable;
large cubes still require sufficient temporary disk space and numerical working
memory. Output paths are checked for collisions and escaping the run directory.

Troubleshooting and checks
--------------------------

* **Unresolved tags:** check environment roots and LUT type/template declarations.
  Keep source date formatting deferred until source-cycle selection.
* **Dry run succeeds but composition fails:** planning checks file presence only,
  and the CLI checks a limited window. Inspect actual cube times and active cells.
* **Forecast file not found after midnight:** use cycle tags for cubes and validity
  tags for separated files. Check ``reference_mode`` and the forecast issue clock.
* **Missing mandatory first step:** supply valid data at that boundary; later-step
  tolerance does not waive the first-step requirement.
* **Grid or units rejected:** check terrain, coordinate names, CRS and conversion
  settings; enable spatial interpolation only when appropriate.
* **Prepared-file copy rejected:** all variables must share a complete model-ready
  file. Use ``as_is: false`` when assembly or transformation is needed.
* **Run folder already exists:** select a fresh destination and keep the earlier
  manifest for comparison.

Run the dataset suite in the scientific environment:

.. code-block:: bash

   python -B -m unittest discover -s tests/dataset -p 'test_*.py' -v

The suite exercises time windows, mode/member handling, file planning, fallback,
source formats, geography, gap policies and validated model forcing using small
synthetic datasets. It does not establish scientific forecast skill or replace
an operational run. The source-level reference is
``workflow/runner/datasets/README.md``; see :doc:`development` for other checks.
