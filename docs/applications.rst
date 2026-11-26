Applications and workflow entry points
======================================

This guide is an index of the runnable applications maintained in
``workflow/``. It groups them by the task they perform and names the matching
settings files where they are shipped. The settings are examples: review their
paths, domain, timestamps, source variables, units and destinations before
running them.

Start with the end-to-end :doc:`runner_workflows` guide if preparing model
forcing and launching HMC or S3M. Use the task guides below for individual
applications.

Running a workflow application
------------------------------

Most conversion, merge, geography, analyzer and dataset-runner applications
read a JSON settings file and an optional reference time. Run from the repository
root in the SHYBOX environment:

.. code-block:: bash

   python workflow/dataset/convert/app_converter_workflow_hmc_destine_icon.py \
     -settings_file workflow/dataset/convert/app_converter_workflow_hmc_destine_icon.json \
     -time "2026-11-09 10:00"

The reference time and settings are application-specific. Inspect each script's
example invocation and the corresponding JSON before launching; example paths
often refer to a particular domain or operational data layout. The dedicated
guides describe configuration fields and safe preview or dry-run options where
available.

Download observations
---------------------

The observation downloader in ``workflow/downloader/`` queries PostgreSQL and
writes hourly station CSVs. The supplied VDA profiles cover local SSH
forwarding and direct server connections. See :doc:`downloader_workflows`
for variable options, examples and realtime or history execution.

.. toctree::
   :maxdepth: 2
   :hidden:

   downloader_workflows

Prepare datasets
----------------

Conversion applications
~~~~~~~~~~~~~~~~~~~~~~~~

The converters in ``workflow/dataset/convert/`` adapt source products to the
grid, time-series and variable layouts expected by downstream workflows.

.. list-table:: Dataset conversion applications
   :header-rows: 1
   :widths: 37 35 28

   * - Application
     - Operation
     - Example settings
   * - ``app_converter_workflow_hmc_destine_icon.py``
     - Convert DESTINE ICON forecasts to HMC grid forcing.
     - ``app_converter_workflow_hmc_destine_icon.json``
   * - ``app_converter_workflow_hmc_destine_ifs.py``
     - Convert DESTINE IFS forecast fields to HMC grid forcing.
     - ``app_converter_workflow_hmc_destine_ifs.json``
   * - ``app_converter_workflow_hmc_fp_icon_vda.py``
     - Convert Flood-PROOFS ICON inputs for the VdA HMC workflow.
     - ``app_converter_workflow_hmc_fp_icon_vda.json``
   * - ``app_converter_workflow_hmc_grid2ts_main.py``
     - Extract grid fields at configured sections or points into time series.
     - ``app_converter_workflow_hmc_grid2ts_marche.json``
   * - ``app_converter_workflow_hmc_itwater_forcing.py``
     - Prepare HMC forcing from the Italy Water input layout.
     - ``app_converter_workflow_hmc_itwater_forcing.json``
   * - ``app_converter_workflow_hmc_itwater_updating.py``
     - Prepare HMC state/update fields from Italy Water inputs.
     - ``app_converter_workflow_hmc_itwater_updating.json``
   * - ``app_converter_workflow_hmc_iwrn.py``
     - Convert IWRN input products into HMC-compatible forcing.
     - ``app_converter_workflow_hmc_iwrn.json``
   * - ``app_converter_workflow_s3m_itwater_forcing.py``
     - Prepare S3M forcing from the Italy Water input layout.
     - ``app_converter_workflow_s3m_itwater_forcing.json``
   * - ``app_converter_workflow_obs_point2grid_rain_main.py``
     - Interpolate point rainfall observations to a grid.
     - ``app_converter_workflow_obs_point2grid_rain_vda.json``
   * - ``app_converter_workflow_obs_point2grid_airt_main.py``
     - Interpolate point air-temperature observations to a grid.
     - ``app_converter_workflow_obs_point2grid_airt_vda.json``
   * - ``app_converter_workflow_obs_point2grid_hs_main.py``
     - Interpolate point snow-height observations to a grid.
     - ``app_converter_workflow_obs_point2grid_hs_vda.json``
   * - ``app_converter_workflow_obs_point2ts_main.py``
     - Convert point observations to section/time-series products.
     - ``app_converter_workflow_obs_point2ts_marche.json``; ``..._po.json``

For operational ICON/IFS workflows, validate source cycles, coordinate names,
units and output periods against the model inputs. Point interpolation settings
also define station metadata, interpolation limits and the target reference
grid. Do not reuse one domain's paths for another domain without checking its
registry and geometry.

Merging and rainfall disaggregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Dataset merge and disaggregation applications
   :header-rows: 1
   :widths: 37 42 21

   * - Application
     - Operation
     - Example settings
   * - ``app_merger_by_domain_workflow_hmc_et.py``
     - Merge HMC evapotranspiration fields by domain.
     - ``app_merger_by_domain_workflow_hmc_et.json``
   * - ``app_merger_by_domain_workflow_hmc_rt.py``
     - Merge HMC return-time fields by domain.
     - ``app_merger_by_domain_workflow_hmc_rt.json``
   * - ``app_merger_by_domain_workflow_hmc_sm.py``
     - Merge HMC soil-moisture fields by domain.
     - ``app_merger_by_domain_workflow_hmc_sm.json``
   * - ``app_merger_by_domain_workflow_s3m_base_main.py``
     - Merge S3M fields across configured domains.
     - ``app_merger_by_domain_workflow_s3m_base_example.json``
   * - ``app_merger_by_time_workflow_s3m_base_main.py``
     - Merge S3M data across configured times.
     - ``app_merger_by_time_workflow_s3m_base_example.json``
   * - ``app_rfarm_workflow_hmc_icon.py``
     - Create RainFARM rainfall ensembles from ICON input.
     - ``app_rfarm_workflow_hmc_icon.json``

Merge applications require aligned variable names, grids, times and no-data
values. The RainFARM application depends on the optional RFarm integration and
its ancillary configuration. Its implementation is under active development;
review the current workflow status before relying on it operationally.

See :doc:`dataset_execution`, :doc:`dataset_planning` and :doc:`dataset_modes`
for the supported HMC/S3M dataset runners, deterministic/probabilistic
separation, time planning and member output checks.

Geography and output analysis
-----------------------------

``workflow/geo/app_geo_workflow_hmc_catchment_main.py`` prepares catchment
geography from a configured reference raster and outlet/section information.
Its example settings are in
``workflow/geo/app_geo_workflow_hmc_catchment_marche.json``. Confirm projection,
grid orientation, pour-point coordinates and expected output layers before
using the result in a model domain. This application does not prepare dynamic
forcing datasets.

``workflow/analyzer/app_analyzer_workflow_hmc_time_series_main_deterministic.py``
joins deterministic HMC output with observations and section data.
``workflow/analyzer/app_analyzer_workflow_hmc_time_series_main_probabilistic.py``
joins configured HMC ensemble members with shared observations. Four example
settings cover JSON and NetCDF output for both modes:

* ``app_analyzer_workflow_hmc_time_series_destine_marche_deterministic_json.json``
* ``app_analyzer_workflow_hmc_time_series_destine_marche_deterministic_netcdf.json``
* ``app_analyzer_workflow_hmc_time_series_destine_marche_probabilistic_json.json``
* ``app_analyzer_workflow_hmc_time_series_destine_marche_probabilistic_netcdf.json``

The JSON settings select source runs, observation registry, variables, output
format and section naming. See :ref:`hmc-time-series-analysis` for the commands,
member configuration and output layouts.

Run HMC and S3M
---------------

Dataset preparation entry points:

* ``workflow/runner/datasets/app_runner_workflow_hmc_datasets_main.py`` — HMC
  observed/forecast forcing, including the configured deterministic or
  probabilistic rainfall profile.
* ``workflow/runner/datasets/app_runner_workflow_s3m_datasets_main.py`` — S3M
  deterministic observed and forecast forcing.
* ``workflow/runner/datasets_mode/app_runner_workflow_hmc_datasets_main.py`` —
  HMC deterministic staging with per-member probabilistic updates and a
  reusable workspace cache.

Model execution entry points:

* ``workflow/runner/execution/app_runner_workflow_hmc_base_main.py`` — prepare
  and launch a base HMC run.
* ``workflow/runner/execution/app_runner_workflow_hmc_orchestrator_main.py`` —
  orchestrate HMC member runs from the configured ensemble workflow.
* ``workflow/runner/execution/app_runner_workflow_s3m_base_main.py`` — prepare
  and launch a base S3M run.

Model executables and model-compatible libraries must be provided separately.
Use :doc:`runners` for model/version settings, executable paths, namelist
inspection and launch commands. Use :doc:`dataset_summaries` to check forcing
files before execution.

The parallel dataset implementation in
``workflow/runner/datasets_parallel/`` is an experiment, not a supported
application in this documentation edition. The old S3M domain merger under
``workflow/dataset/merge/old/`` is retained for reference. Historical runner
files under ``apps/runner/`` target the HMC 3.1.6 example. Its
``apps/runner/app.py`` is a legacy Flask form interface, not the current model
runner; do not expose it as an operational service. Use the current
``workflow/`` entry points above for new runs.

Monitoring applications and interfaces
---------------------------------------

The monitoring examples are grouped under ``workflow/monitoring/``:

* ``app_monitoring_main_example.py`` demonstrates start/heartbeat/finish
  lifecycle records.
* ``app_monitoring_memory_example.py`` samples configured process and host CPU
  and RAM and can make a RAM plot.
* ``app_monitoring_status_demo.py`` creates synthetic run states for Hydrapp;
  ``--resources`` also samples the demo process.
* ``app_monitoring_ruler_example.py`` runs the native Ruler report on configured
  processes and resources.
* ``app_monitoring_ruler_demo.py`` creates bounded sample data for the native
  Ruler report.
* ``app_monitoring_ruler_workload.py`` provides the bounded process workload
  used by monitoring examples.

Commands and JSON examples are in :doc:`monitoring`. Hydrapp is the current
browser interface:

.. code-block:: bash

   python -m hydrapp.app --config hydrapp/config.demo.json --port 5050

Open ``http://127.0.0.1:5050`` for the demo dashboard. Read :doc:`hydrapp` for
data locations, page/API routes and production configuration. The older Flask
application under ``web/`` is retained as a legacy interface, not the current
monitoring dashboard.

Standalone workflow APIs
------------------------

Most ``workflow/`` applications compose SHYBOX toolkits through configuration.
For reusable interfaces and import examples, see :doc:`toolkits`. To make a
small custom application, begin with :doc:`configuration`, choose a local data
handler from :doc:`toolkits`, and follow the existing workflow's process list
and settings structure. Copy an example settings file and change it for the
target data; avoid importing implementation code from ``workflow/`` when a
stable toolkit API exists.
