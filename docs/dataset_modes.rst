Reuse deterministic forcing across HMC members
==============================================

The separate entry point under ``workflow/runner/datasets_mode/`` uses the HMC
dataset-runner template in three stages. The existing scripts and JSONs under
``workflow/runner/datasets/`` remain unchanged.

1. Filter the adjusted plan by source type. Process deterministic observed fields
   and deterministic forecast fields once per time and output collection into
   plain NetCDF files in the workspace.
2. For each member, copy the matching observed deterministic file to its final
   observed path, then copy each matching forecast deterministic file to its
   final forecast path.
3. Process only probabilistic fields for each member and expected forecast step,
   merge them into that member's forecast copy, and save its summary when all its
   times finish.

For the supplied HMC rainfall ensemble, observed cache files contain all five
fields. Forecast cache files contain temperature, radiation, humidity and wind;
member-specific rainfall is added after copying. An entirely deterministic plan
needs no member updates. An entirely probabilistic forecast needs no shared base.

Run with the existing HMC configuration
---------------------------------------

Set the same environment roots as :doc:`dataset_execution`, and use an edited
copy of either HMC forcing JSON. The source ``mode`` and ``ensemble`` declarations
determine which fields are shared. No extra JSON keys are required.

.. code-block:: bash

   python -m workflow.runner.datasets_mode.app_runner_workflow_hmc_datasets_main \
     -settings_file /absolute/path/to/hmc_forcing_settings.json \
     -time "2026-11-09 10:00" \
     --workspace /work/my_domain/tmp/hmc_modes

The default selects both observed and forecast times. Use ``--time-mode forecast``
or ``--time-mode observed`` to restrict processing. The workspace defaults to
``application.tmp.path/dataset_modes`` and must be separate from final output
directories. Planning history is kept inside that workspace. Final forcing,
compression and member-summary paths still follow the supplied JSON.

Deterministic cache entries are reused on subsequent invocations when their
upstream path/size/timestamps, geographic input and processing configuration match.
Each entry has a manifest and content hash; missing, incomplete or damaged entries
are rebuilt. Use ``--no-reuse`` to force deterministic processing again.
Probabilistic fields are processed for each member on every invocation.
Use a separate workspace for concurrent runs; this entry point executes sequentially.

Shared and probabilistic workflows must be independently executable. A rainfall
chain cannot consume an intermediate from a separate deterministic chain.
The runner chooses ``_WORKFLOW_OBS`` or ``_WORKFLOW_FRC`` for the current time mode,
then a generic ``_WORKFLOW`` name. Observations may fall back to ``_WORKFLOW_FRC``
to support the existing HMC pass-through JSON; define explicit observation chains
when their scientific processing differs.

Planner and storage interfaces
------------------------------

``planning_handler.DatasetPlanner`` keeps its existing public interface. Its
unchanged base implementation lives in ``planning_base_handler.py``;
``planning_mode_handler.py`` adds source-mode selection:

.. code-block:: python

   shared = planner.file_plan_by_mode('deterministic')
   member_rain = planner.file_plan_by_mode(
       'probabilistic', time_mode='forecast', member='001')
   source = planner.source_settings('rain', 'forecast')
   destination = planner.destination_settings('rain', 'forecast')

Deterministic selection deduplicates copies of the same variable/time/collection
across member destinations. Use ``unique=False`` to retain those destination rows.
The retained path in a deduplicated selection is representative, not a shared
output target. These methods return copies and do not change the base file plan,
availability checks or observed/forecast fallback decisions.

``lib_utils_mode.py`` supplies ``DatasetModeStore`` for shared-file reuse,
``local_source`` for private gzip expansion and ``publish_member`` for copying and
updating member files. It appends only selected output variables, checks dimensions
and time/grid coordinates, preserves shared variables and metadata, and replaces
the final member file atomically. A failed update leaves the previous output
intact. Final outputs cannot alias upstream inputs or cache files, and different
probabilistic members must have distinct output paths.

The HMC writer recreates files rather than appending to them. Therefore this
runner writes probabilistic fields to a separate file and merges them into the
member copy using NetCDF operations. The dataset writer now preserves a
single-variable mapping for NetCDF outputs, so its configured variable name is
available to the HMC writer. Verify completed files using
:doc:`dataset_summaries` before following the model launch guide in :doc:`runners`.
