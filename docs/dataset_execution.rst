Executing the dataset runner
============================

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
