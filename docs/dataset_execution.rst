Executing the dataset runner
============================

The sequential dataset runner prepares forecast forcing from already available
upstream products. Rainfall may vary by ensemble member while temperature,
radiation, humidity and wind come from shared deterministic sources. Every
member/time orchestrator assembles all configured variables for its output file.
It does not launch the hydrological or snow model; continue with :doc:`runners`
after checking the output manifest in :doc:`dataset_summaries`.

Choose a profile and environment
--------------------------------

Run commands from the repository root using the scientific environment in
:doc:`installation`. Copy a profile before editing its time horizons, field
names, units, grids or processing methods.

.. list-table:: Profiles in ``workflow/runner/datasets/``
   :header-rows: 1
   :widths: 25 75

   * - Application
     - Settings filename
   * - HMC deterministic
     - ``app_runner_workflow_hmc_datasets_forcing_deterministic.json``
   * - HMC probabilistic rainfall
     - ``app_runner_workflow_hmc_datasets_forcing_probabilistic.json``
   * - S3M deterministic
     - ``app_runner_workflow_s3m_datasets_forcing.json``

Set the environment roots referenced by the selected settings LUT:

.. code-block:: bash

   source ./conda/shybox_base_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
   export DOMAIN_NAME="my_domain"
   export PATH_GEO="/data/my_domain/geo"
   export PATH_OBS="/data/my_domain/observed"
   export PATH_FRC_NWP="/data/my_domain/nwp"
   export PATH_FRC_RFARM="/data/my_domain/rainfarm"
   export PATH_DST="/work/my_domain/forcing"
   export PATH_LOG="/work/my_domain/log"

``PATH_FRC_RFARM`` is needed for the probabilistic rainfall profile. The terrain
file must match the configured geometry and the processed output grid. Source
``variable`` maps raw names to canonical names; destination ``variable`` maps
canonical names to model fields. Review the output path template before running:
the probabilistic profile uses ``run_{ensemble_member}``, so members write to
separate folders. Time tags are resolved using the planner's LUT date formats.

Preview the plan without processing
-----------------------------------

.. code-block:: bash

   python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
     -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
     -time "2026-11-09 09:35" --dry-run --plan-files

Both ``--dry-run`` and ``--plan-files`` select planning-only behavior. The command
checks the configured availability window, applies any forecast fallback and
constructs the forecast handlers/orchestrators without reading scientific arrays
or writing forcing, summary or history files. It can create log files.
``--plan-files`` adds file-plan rows to the JSON printed after the log output.
The plan covers observed and forecast validity times; configured workflow counts
cover forecast steps, matching actual execution.

By default the entry point checks signed steps -3 through +1. These checks do
not limit the execution interval: the runner processes every forecast step in
the adjusted plan. ``None`` limits disable a side of the availability check;
settings in ``configuration.application.planning`` override the defaults.
Review :doc:`dataset_planning` for run rounding, source cycles and gap replacement.

Configure scientific workflows
------------------------------

The top-level ``workflow.process_list`` supplies each canonical variable's chain.
The runner selects ``<VARIABLE>_WORKFLOW_FRC`` for forecasts when present, otherwise
``<VARIABLE>_WORKFLOW``. Python setup for observations selects the corresponding
``_WORKFLOW_OBS`` or generic name. Names are uppercase. The supplied probabilistic
profile uses the forecast suffix; deterministic/S3M profiles use generic names.

.. code-block:: json

   {
     "workflow": {
       "options": {
         "io_config": {"mode": "many_data_files_to_one_file"},
         "intermediate_output": "Memory"
       },
       "process_list": {
         "RAIN_WORKFLOW_FRC": [{
           "function": "mask_data_by_limits",
           "datasets": {"data": "rain"},
           "results": {"data": "Rain"},
           "args": {"mask_min": null, "mask_max": null, "mask_format": null}
         }]
       }
     }
   }

Define a nonempty chain for every planned variable. ``datasets`` refers to
canonical input fields; final ``results`` names must match model output fields.
Sample chains pass through values. Add interpolation, masking, unit conversion
or other scientific processing as your upstream product requires. Configuration
metadata such as ``operations`` does not automatically create processing steps.
``workflow.priority`` can order the active variable chains.

Process discovery checks SHYBOX directories matching ``processing*_toolkit``.
Use ``workflow.process_locations`` to add
custom directories or dotted Python modules/packages. Only requested methods
are loaded; missing or ambiguous methods fail during configuration. The shared
``config_process_by_variables()`` utility accepts a format template such as
``workflow_template='{variable}_WORKFLOW_FRC'`` for other procedures.

Run sequential forcing preparation
----------------------------------

Remove the planning flags to execute:

.. code-block:: bash

   python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
     -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
     -time "2026-11-09 09:35"

This reads arrays and writes to the configured destinations. It iterates members,
then forecast times, using a source/destination pair per variable and one combined
orchestrator per member/time. The CLI processes forecast mode only, including
observed times that were successfully replaced by forecasts. Unadjusted observed
files remain outside this execution loop.

For each step, execution tracing retains status, UTC start/finish timestamps,
duration, source/destination paths, output-presence flags and workflow names.
No handlers, orchestrator objects or arrays are saved in that trace. Timing uses
a monotonic clock for elapsed seconds and UTC timestamps for dates.

An execution exception is logged and re-raised; later times/members do not run.
A summary is published only after the current member's time loop completes.
Output-presence failure generates an incomplete summary and a warning; consumers
must check its ``ready`` field. A successful Python exit does not establish
scientific validity of the produced data. See :doc:`dataset_summaries` for the
handoff to model execution.

History and output options
--------------------------

The CLI defaults to reusing compatible planning history:
``flag_history=True`` and ``reset_history=False``. The default workspace is
``plans_file.workspace`` beside the selected settings file. Set
``application.planning.pickle_history`` to an explicit run-specific path.
Relative configured paths are relative to the working directory.

.. code-block:: json

   {
     "planning": {
       "n_step_to_check_obs": 3,
       "n_step_to_check_frc": 1,
       "flag_adjust_obs_frc": true,
       "rules_to_fill_obs": "fill_using_frc",
       "rules_to_fill_frc": null,
       "flag_history": true,
       "reset_history": true,
       "pickle_history": "/work/my_domain/run/plans_file.workspace"
     },
     "summary_merge": false,
     "summary_file": "/work/my_domain/run/model_inputs.{member}.json",
     "summary_extra_args": {"scenario": "control"}
   }

Place those keys inside ``configuration.application``. Use
``reset_history=True`` to refresh input availability when files may have arrived
since a prior check. Set ``flag_history=False`` to avoid workspace I/O. Preview
mode always disables history I/O. Only load trusted planning pickle files.

Destination ``file_args`` forwards writer options, including
``file_compression`` for a gzip wrapper and ``compression_flag`` for internal
NetCDF compression. The destination's ``as_compress`` is a fallback for
``file_compression``. The writer receives the base filename without its ``.gz``
suffix; the summary accepts either the base file or the gzip counterpart and
records the actual one found. HMC metadata keeps numeric missing-value markers
in the variable's dtype, avoiding mixed string/numeric decoding warnings in
newly written files.

Reuse setup from Python
-----------------------

``configure_dataset_workflows()`` constructs workflows without running them:

.. code-block:: python

   import pandas as pd
   from workflow.runner.datasets.app_runner_workflow_hmc_datasets_main import configure_dataset_workflows

   configured = configure_dataset_workflows(
       planner, workflow_configuration, time_mode='forecast',
   )
   for member, times in configured.items():
       for time_key, step in times.items():
           step['orchestrator'].run(time=pd.Timestamp(time_key), group='by:step')
       # Summarize this member after all of its steps; see dataset_summaries.

The return structure is
``{member: {time_string: {'source': handlers, 'destination': handlers, 'orchestrator': obj}}}``.
The default ``time_mode=None`` includes both groups; choose observed or forecast
explicitly when integrating another procedure. A deterministic plan uses the
member key ``deterministic``. Keep complete output files under one orchestrator;
separate writers for variables of the same file can overwrite each other's data.
The local ``datasets_parallel`` experiment is excluded from SHYBOX 1.7.1.
