runner/datasets: examples and configuration
===========================================

The HMC sequential runner prepares deterministic or probabilistic forecast forcing.
The separate S3M runner prepares deterministic observed and forecast forcing.
Both use the dataset planner, process each member's times sequentially and write
one output-presence summary after each member completes. They do not launch the
hydrological or snow model; continue with :doc:`runners` after checking the
manifest in :doc:`dataset_summaries`.

Environment and profiles
------------------------

Run from the repository root using the scientific environment in :doc:`installation`.
Set ``DOMAIN_NAME``, ``PATH_GEO``, ``PATH_OBS``, ``PATH_FRC_NWP``, ``PATH_DST``,
``PATH_LOG``, ``PATH_TMP``, ``PATH_PLANNING`` and ``PATH_SUMMARY``. Probabilistic
HMC rainfall also needs ``PATH_FRC_RFARM``. Terrain, field names, units and grids
must agree with the chosen upstream products.

.. list-table:: Files in ``workflow/runner/datasets/``
   :header-rows: 1
   :widths: 20 40 40

   * - Case
     - Python entry point
     - Settings JSON
   * - HMC deterministic
     - ``app_runner_workflow_hmc_datasets_main.py``
     - ``app_runner_workflow_hmc_datasets_forcing_deterministic.json``
   * - HMC rainfall ensemble
     - Same HMC entry point
     - ``app_runner_workflow_hmc_datasets_forcing_probabilistic.json``
   * - S3M deterministic
     - ``app_runner_workflow_s3m_datasets_main.py``
     - ``app_runner_workflow_s3m_datasets_forcing.json``

Copy the selected JSON into a run directory before adapting it. For example,
prepare a deterministic HMC case from the repository root:

.. code-block:: bash

   source ./conda/shybox_base_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
   export FORCING_RUN_ROOT="$PWD/exec/my_domain/forcing_run"
   export DOMAIN_NAME="my_domain"
   export PATH_GEO="/absolute/path/to/terrain"
   export PATH_OBS="/absolute/path/to/observations"
   export PATH_FRC_NWP="/absolute/path/to/weather/forecasts"
   export PATH_DST="$FORCING_RUN_ROOT/forcing"
   export PATH_LOG="$FORCING_RUN_ROOT/log"
   export PATH_TMP="$FORCING_RUN_ROOT/tmp"
   export PATH_PLANNING="$FORCING_RUN_ROOT/planning"
   export PATH_SUMMARY="$FORCING_RUN_ROOT/summaries"
   mkdir -p "$FORCING_RUN_ROOT"
   cp workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
      "$FORCING_RUN_ROOT/settings.json"

Before execution, edit ``configuration.application`` in that copy. Set the terrain
filename (the examples use ``{domain_name}.dem.txt``), observed/forecast path and
filename templates, raw variable names and coordinate names. The user LUT under
``configuration.variables.lut.user`` selects time windows and source issue clocks.
Check ``data_destination.collections`` and ``workflow.process_list`` against the
field names, units and grid expected by the model. Exported paths must point to
existing upstream products; SHYBOX does not download them.

Run the copied HMC profile:

.. code-block:: bash

   python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
     -settings_file "$FORCING_RUN_ROOT/settings.json" \
     -time "2026-11-09 10:00"

For a rainfall ensemble, use the probabilistic JSON in a separate run directory,
export ``PATH_FRC_RFARM`` to the rainfall-member root and set ``ensemble_start``,
``ensemble_end`` and ``ensemble_format`` in its user LUT. Rainfall varies by
member; other meteorological fields can remain shared. The example destinations
contain ``run_{ensemble_member}`` to keep member files separate.

For S3M, export the same roots for your snow domain, copy its JSON into a separate
run directory and invoke the S3M entry point. The two original-profile commands
below illustrate the matching script/settings pairs; use your edited copies for
actual domains:

.. code-block:: bash

   python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
     -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
     -time "2026-11-09 10:00"

   python -m workflow.runner.datasets.app_runner_workflow_s3m_datasets_main \
     -settings_file workflow/runner/datasets/app_runner_workflow_s3m_datasets_forcing.json \
     -time "2026-11-09 10:00"

For HMC probabilistic rainfall, choose
``app_runner_workflow_hmc_datasets_forcing_probabilistic.json`` with the HMC entry
point. The S3M entry point rejects probabilistic sources before availability checks
or processing. The scripts accept ``-settings_file`` and ``-time`` and execute
immediately. Use ``DatasetPlanner`` directly for metadata-only inspection.

Time windows and missing observations
-------------------------------------

``-time`` overrides the configured run and is rounded using ``time_rounding``.
Planner times are naive UTC. With a rounded run of 10:00, a 3h observed look and
a null observed horizon, the observed interval is 07:00 through 10:00 inclusive.
A 3h forecast look and null forecast horizon select 11:00 through 13:00.
``"midnight"`` applies the day-boundary policy; an explicit ``"12:00"`` selects
that clock boundary on the run date and must fit inside the corresponding look
window. See :doc:`dataset_planning` for examples and invalid combinations.

The shipped planning options check steps -3 through +1, including zero. They do
not check every planned input. A missing ``ws.db`` at 10:00 can move to forecast
if it belongs to the trailing observed gap and every required forecast input is
available for all members. The log reports the replacement; ``time_adjusted``
and the rebuilt file paths record it. A missing forecast candidate leaves that
step observed and unavailable. An internal gap followed by newer available
observations does not move the cutoff. Disable history reuse while diagnosing
newly arriving observations, or set ``planning.files.reset_history`` to true.

Planning, history and summaries
-------------------------------

The three profiles use the same nested configuration:

.. code-block:: json

   {
     "planning": {
       "options": {
         "n_step_to_check_obs": 3,
         "n_step_to_check_frc": 1,
         "flag_adjust_obs_frc": true,
         "rules_to_fill_obs": "fill_using_frc",
         "rules_to_fill_frc": null
       },
       "files": {
         "path": "{path_planning}/auxiliary/{path_time_planning}",
         "file_name": "planning_execution_{file_time_planning}.workspace",
         "reset_history": true,
         "flag_history": true
       },
       "summary": {
         "path": "{path_summary}/summary/{path_time_summary}",
         "file_name": "summary_execution_{file_time_summary}_deterministic.json"
       }
     }
   }

``update_time()`` uses the planner's stored options unless overridden by explicit
call arguments. The check window controls availability probes; it does not limit
execution times. Missing trailing observations switch to forecast only when all
required forecast inputs are available. See :doc:`dataset_planning` and
:doc:`datasets` for look windows and clock-time horizons.

``reset_history: true`` refreshes checks; false reuses a compatible saved plan
without probing files. ``flag_history: false`` disables history I/O. Only reuse
trusted pickle files. Summary files are atomic JSON writes and probe destination
presence afresh. The planner exposes ``file_planning`` after updating,
``file_summary`` after saving the latest member, and ``files_summary`` as a
member-to-path dictionary. These paths are absolute and have filled placeholders.

The HMC runner processes and summarizes forecast steps, including observed gaps
successfully replaced by forecasts. S3M processes and summarizes both modes.
Execution history stores completed timestamps under ``execution``. Exceptions
stop the run before that member's summary is published. An incomplete manifest
produces a warning; consumers must check ``ready`` before model execution.
Use ``summary_execution()`` separately to merge completed member summaries.

Scientific workflows and output
-------------------------------

``data_destination.collections`` defines the output coordinates, variables, units
and ``file_args``. HMC uses ``hmc_forcing``; S3M uses ``forcing_s3m``. The profiles
write all variables for each time to one file, optionally gzip compressed through
``file_args.file_compression``.

``workflow.process_list`` supplies nonempty chains for every canonical variable.
Forecast names are ``<VARIABLE>_WORKFLOW_FRC``. S3M also defines
``<VARIABLE>_WORKFLOW_OBS`` and activates only the mode needed for each time.
Source ``variable`` maps raw fields to canonical names; process results and
destination mappings select the model field names. Sample chains pass values
through. Configure interpolation, masking and unit conversion explicitly;
source ``operations`` metadata does not automatically add processing methods.

``workflow.process_locations`` lists modules searched for processing functions.
``workflow.options.tmp_dir`` selects temporary storage. Every member/time
orchestrator owns its complete output file; members use distinct destination paths.

.. _runner-dataset-handoff:

Check outputs and hand them to model execution
----------------------------------------------

After a successful member finishes, find its JSON under ``PATH_SUMMARY``. The
shipped templates use a dated ``summary/YYYY/MM/DD/`` subfolder. Inspect one
explicit file before selecting a model interval:

.. code-block:: bash

   export MEMBER_SUMMARY="$PATH_SUMMARY/summary/2026/11/09/summary_execution_202611091000_deterministic.json"
   python - "$MEMBER_SUMMARY" <<'PY'
   import json
   from pathlib import Path
   import sys

   record = json.loads(Path(sys.argv[1]).read_text())
   if not record['ready']:
       raise SystemExit(f"Incomplete forcing for {record['member']}: {record['missing_times']}")
   print('Member:', record['member'])
   print('Forcing bounds:', record['time_start'], record['time_end'])
   print('Steps:', record['time_steps'])
   for step in record['steps']:
       for variable, details in step['variables'].items():
           print(variable, details['file_actual'])
   PY

``ready`` checks file presence, including accepted gzip counterparts. Inspect
the NetCDF variables, units and grid as well. :doc:`dataset_summaries` explains
the full schema, custom metadata, merging and Python read/write methods.

Then copy a model JSON from ``workflow/runner/execution/`` and follow
:doc:`runners`. Connect the stages explicitly:

.. list-table:: Dataset-to-model handoff
   :header-rows: 1
   :widths: 35 65

   * - Prepared output
     - Model configuration to review
   * - Actual paths in ``steps[*].variables[*].file_actual``
     - Set ``application_namelist.fields.by_value.sPathData_Forcing_Gridded`` to the matching dated layout; preserve model ``$yyyy/$mm/$dd`` tokens where needed.
   * - ``time_start``, ``time_end`` and forcing frequency
     - Set the model simulation interval and timestep to covered inputs; inspect the generated ``sTimeStart`` and ``iSimLength``.
   * - Geographic grid, field names and units
     - Match model forcing format, static grids and relevant namelist flags.
   * - Ensemble member identifier
     - Select that member's forcing and isolate its executable, namelist, restart, logs and results.
   * - Initial model state
     - Select cold start or supply a compatible restart at the model-required time; the dataset manifest does not create restart files.

HMC prepares forecast steps only. Supply any existing observed forcing required
by your chosen model interval. S3M prepares both modes, and its sample layout
keeps ``observed`` and ``forecast`` in separate directories. Model examples use
one forcing-root pattern: consolidate/link the required files into the model's
expected dated layout, or adapt the dataset destination template before running.
Do not assume that setting the model's ``PATH_SRC`` to the dataset ``PATH_DST``
alone makes the two layouts compatible. The dataset and model stages can also
interpret timezones differently; check the time conventions in :doc:`runners`.

Troubleshooting
---------------

* **Unresolved path tags:** export all roots declared in the selected JSON's
  environment LUT; include ``PATH_TMP``, ``PATH_PLANNING`` and ``PATH_SUMMARY``.
  Keep deferred time/member tags in templates and declare their LUT formats.
* **Observation missing but no forecast replacement:** check the configured
  availability window, fallback rule, trailing-gap condition, forecast issue
  time and presence of every required member input. Refresh stale history.
* **Clock-time horizon rejected:** choose a boundary within the observed or
  forecast look window and aligned with ``time_frequency``.
* **Workflow or field not found:** supply a nonempty chain for every canonical
  variable using the correct ``_FRC`` or ``_OBS`` suffix and raw/model mappings.
* **Incomplete summary:** inspect its missing steps and file paths. A successful
  Python exit can still leave an incomplete manifest; require ``ready`` explicitly.
* **Model cannot find prepared forcing:** compare the manifest paths with the
  generated namelist, including mode/member directories, dates and compression.
