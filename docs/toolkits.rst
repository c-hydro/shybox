SHYBOX Python toolkits
======================

SHYBOX applications are composed from small packages under ``shybox/``. Use
these packages when building a new Python workflow; use the runnable examples in
:doc:`applications` when you need a complete settings file and command.

The table lists every toolkit package in this repository, grouped by its place
in a workflow. Processing adapter functions are usually selected by name in an
orchestrator process list; direct utility functions and handlers are imported
from their package modules.

Toolkit directory map
---------------------

Configuration, time and data access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Foundation toolkits
   :header-rows: 1
   :widths: 23 42 35

   * - Package
     - Use it for
     - Main interface
   * - ``config_toolkit``
     - Load JSON settings, validate sections, resolve lookup tables, environment
       values and placeholders.
     - ``ConfigManager``; ``ApplicationConfig``; ``ArgumentsManager``
   * - ``time_toolkit``
     - Normalize timestamps, resolve run windows, intervals and restart times.
     - ``TimeManager`` and ``lib_utils_time``
   * - ``dataset_toolkit``
     - Describe file, memory, on-demand, temporary and grouped datasets; map data
       to configured coordinates and variables.
     - ``DataLocal``, ``DataMemory``, ``DataOnDemand``, ``DataTemporary``,
       ``DataCollection``
   * - ``io_toolkit``
     - Read and write NetCDF, GRIB, ASCII grids, HMC sections and time-series
       JSON; convert file-specific layouts.
     - ``lib_io_utils``, ``lib_io_nc_*``, ``lib_io_ascii_*``,
       ``json_wrapper``
   * - ``geo_toolkit``
     - Inspect and normalize coordinate arrays, grid orientation and watershed
       geometry helpers.
     - ``lib_geo_coords``, ``lib_geo_utils``, ``lib_geo_watersheds``
   * - ``generic_toolkit``
     - Shared dictionary, debug workspace, plotting and small code utilities.
     - ``lib_utils_dict``, ``lib_utils_debug``, ``lib_utils_code``
   * - ``logging_toolkit``
     - Configure the shared logger, output handlers and nested progress messages.
     - ``LoggingManager`` and ``with_logger``
   * - ``default``
     - Supply shared default log, time, path and model metadata values.
     - Modules under ``shybox.default``; this support package is not a toolkit.

Processing and orchestration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Processing and workflow toolkits
   :header-rows: 1
   :widths: 26 44 30

   * - Package
     - Use it for
     - Main interface
   * - ``orchestrator_toolkit``
     - Map data namespaces to workflow inputs, discover registered functions,
       run configured process chains and write their outputs.
     - ``OrchestratorConfig``, ``Mapper``, ``config_process_by_variables``
   * - ``processing_grid_toolkit``
     - Interpolate and merge gridded fields; derive humidity, wind and radiation.
     - ``lib_proc_merge``, ``lib_proc_interpolate``,
       ``lib_proc_compute_*``
   * - ``processing_points_toolkit``
     - Interpolate station rainfall, air temperature and snow height; join points
       to section time series and apply grid masks.
     - ``points_adapter`` and ``lib_proc_interpolate_*``
   * - ``processing_ts_toolkit``
     - Join time series by registry and average grid data over masks.
     - ``ts_adapter``, ``join_time_series_by_registry``
   * - ``processing_hydro_toolkit``
     - Compute evapotranspiration, return-time and soil-moisture products.
     - ``hydro_adapter`` and ``lib_proc_compute_*``
   * - ``processing_geo_toolkit``
     - Delineate catchment masks and derive watershed grids.
     - ``delineate_watershed``
   * - ``processing_rfarm_toolkit``
     - Adapt rainfall inputs and call the optional RFarm ensemble generator.
     - ``exec_rfarm_nwp``

Model runners and application integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Runner, monitoring and integration toolkits
   :header-rows: 1
   :widths: 24 43 33

   * - Package
     - Use it for
     - Main interface
   * - ``runner_toolkit``
     - Plan observed/forecast files and member outputs, prepare model namelists
       and drive runner orchestration.
     - ``DatasetPlanner``, ``NamelistCreator``,
       ``NamelistTemplateManager``
   * - ``monitoring_toolkit``
     - Record execution lifecycle and sample process/host resources for Hydrapp
       and standalone workflows.
     - ``RunMonitor``, ``ProcessMemoryMonitor``,
       ``MonitoringCatalog``
   * - ``publishing_toolkit``
     - Read legacy published-figure catalogues used by the viewer.
     - ``LegacyFigureCatalog``
   * - ``external_toolkit``
     - Find and register separately maintained Python integrations and verify
       their availability.
     - ``get_external_config``, ``register_toolkit_path``,
       ``check_external_toolkit``

The ``external_toolkit`` directory can contain an integration adapter, but
third-party packages may be separately installed. Check the selected workflow's
requirements and the relevant ``pyproject.toml`` external-toolkit configuration.

How a workflow uses the toolkits
--------------------------------

A typical application follows these steps:

#. Load settings with ``ConfigManager`` and select the run time with the time
   or runner toolkit.
#. Describe input and output datasets with dataset handlers. Dataset handlers
   delegate format-specific operations to ``io_toolkit``.
#. Configure an ``OrchestratorConfig`` process graph. The process list selects
   adapter functions from the relevant ``processing_*_toolkit`` packages.
#. Run and write results through the configured output handlers; use the runner
   toolkit for model forcing or namelist/executable tasks.
#. Add lifecycle/resource records with ``monitoring_toolkit`` when the
   application needs operational tracking.

Use the shipped application rather than assembling these steps manually when
its settings already match your data:

.. code-block:: python

   from shybox.config_toolkit.config_handler import ConfigManager
   from shybox.dataset_toolkit.dataset_handler_local import DataLocal
   from shybox.monitoring_toolkit.execution import RunMonitor
   from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
   from shybox.time_toolkit.time_handler import TimeManager

Each class has configuration-specific required fields. For complete construction
examples, use :doc:`configuration`, :doc:`dataset_planning`, :doc:`runners` and
:doc:`monitoring`; these guides show valid settings and runtime arguments.

Small examples by task
----------------------

Load and resolve an application configuration:

.. code-block:: python

   from shybox.config_toolkit.config_handler import ConfigManager

   config = ConfigManager.from_source(
       "workflow/runner/datasets/app_runner_workflow_s3m_datasets_forcing.json",
       root_key="configuration",
   )

Given a configured planner, inspect its adjusted file plan and member/time
selection:

.. code-block:: python

   plan = planner.file_plan
   members = planner.members
   member_files = planner.file_by_ensemble("destination", time_mode="forecast")

For member summaries and history persistence, see :doc:`dataset_summaries`.
To read and write formats without an orchestrator, use the dataset handler's
``get_data`` and ``write_data`` methods with a complete variable/coordinate
template; format-specific controls are passed as file arguments. See
:doc:`datasets` for those contracts.

Compatibility and maturity
--------------------------

The public imports and workflow examples are the preferred extension points.
Modules named ``old`` are archived implementations. Some workflow adapters,
especially optional external integrations, are still being completed; check
:doc:`workflows` for known development limitations before depending on them.
