Dataset planning before model execution
=========================================

``DatasetPlanner`` in ``shybox.runner_toolkit.datasets.planning_handler``
organizes observed and forecast filenames before an HMC/S3M run. It computes
validity times and source cycles, expands ensemble members, checks a selected
availability window and can fill the trailing observed gap from forecast files.
Planning does not open data arrays, write forcing files or launch a model.

Use the scientific environment from :doc:`installation`. The planner itself
uses pandas and SHYBOX logging. It does not require the development forcing
composer. Continue with :doc:`runners` for model namelists and execution;
:doc:`dataset_execution` covers the sequential forcing runner and
:doc:`dataset_summaries` covers its JSON handoff to other procedures.

Configure the dataset sections
--------------------------------

Resolve environment/LUT placeholders through your existing configuration flow
before constructing the planner. Keep deferred date and member placeholders
such as ``{file_time_observed}`` and ``{ensemble_member}`` in file templates.
The application dictionary has these sections:

.. list-table:: Dataset planning configuration
   :header-rows: 1
   :widths: 32 68

   * - Section
     - Contents
   * - ``info``
     - Dataset/domain metadata; a dictionary, even when empty.
   * - ``time``
     - Run, rounding, frequency, observed/forecast look and horizon policies.
   * - ``data_source.observed``
     - Named source entries with input-to-canonical variable mappings.
   * - ``data_source.forecast``
     - Forecast sources; may include member-specific rainfall and shared fields.
   * - ``data_destination.observed`` / ``data_destination.forecast``
     - Named output collections with canonical-to-output variable mappings.
   * - ``geo``
     - Optional geography metadata. File planning does not read terrain.

``from_config`` accepts separate source/destination dictionaries for both groups.
``from_application`` accepts the assembled dictionary above and also supports
one shared ``data_destination`` collection dictionary for both groups.

This complete Python example plans one rainfall variable. Add ``air_t``,
``inc_rad``, ``rel_hum`` and ``wind_speed`` for a complete model forcing set,
using the names and units required by your model. Paths are examples:

.. code-block:: python

   from copy import deepcopy
   from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner

   time_settings = {
       "time_run": "2026-11-09 09:35",
       "time_rounding": "1h", "time_frequency": "1h",
       "hour_reference_observed": None, "hour_reference_forecast": "00:00",
       "time_look_observed": "3h", "time_horizon_observed": None,
       "time_look_forecast": "2h", "time_horizon_forecast": None,
       "priority_observed": "look", "priority_forecast": "look",
   }
   observed = {
       "rain": {
           "path": "/data/observed/{path_time_observed}",
           "file_name": "rain.{file_time_observed}.nc",
           "x": "longitude", "y": "latitude", "time": "time",
           "dims": "2d", "nodata": -9999, "format": "netcdf",
           "variable": {"precipitation": "rain"},
           "mode": "deterministic", "ensemble": None, "required": True,
           "tolerance_steps": 0, "missing_policy": "error", "as_is": False,
       }
   }
   forecast = deepcopy(observed)
   forecast["rain"].update(
       path="/data/forecast/{path_time_forecast_nwp}",
       file_name="rain.{file_time_forecast_nwp}.nc", dims="3d",
   )
   destination = {
       "forcing": {
           "path": "/data/forcing/{path_time_run}",
           "file_name": "{time_mode}/forcing.{file_time_destination}.nc",
           "format": "netcdf", "type": "hmc", "dims": "2d",
           "variable": {"rain": "Rain"},
       }
   }
   planner = DatasetPlanner.from_config(
       info={"domain_name": "example", "model": "hmc"}, time=time_settings,
       data_src_obs=observed, data_src_frc=forecast,
       data_dst_obs=destination, data_dst_frc=destination,
   )
   time_table = planner.time_table
   files = planner.plan_files()  # No file-presence checks yet.

Source entries require ``path``, ``file_name``, ``x``, ``y``, ``time``, ``dims``,
``nodata``, ``format``, ``variable``, ``mode``, ``ensemble``, ``required``,
``tolerance_steps``, ``missing_policy`` and ``as_is``. Destination collections
require ``path``, ``file_name``, ``format``, ``type``, ``dims`` and ``variable``.
These metadata describe the files; the planner does not validate their contents
or apply a composition/gap policy.

``dims="2d"`` uses a source's validity step; ``dims="3d"`` uses its issue cycle.
``reference_mode="step"`` or ``"3d"`` overrides reference selection; a source
with ``operations.split`` also defaults to a cube reference. Cube issue times
may precede the selected validity window and stay fixed across midnight.
Use a ``*_valid`` deferred tag when a filename also needs validity time.

Time plans and deferred tags
------------------------------

All planner times are naive UTC. ``time_run`` supplied to the constructor or
``from_config`` overrides the configured run. The time factory rounds the run
and builds observed/forecast windows using the look/horizon priorities described
in :doc:`datasets`. ``configure_time(time_plan=existing_period)`` reuses a
``TimePeriod``; alternatively pass ``time_factory=callable`` at construction.

The planner accepts ``obs``/``frc`` or ``observed``/``forecast`` in an incoming
time plan and normalizes its tables and ``{time_mode}`` tags to
``observed``/``forecast``. The original supplied plan is not modified.
``time_table`` includes sequential ``step``, signed ``time_step``, source
reference columns and ``file_id``. Zero is the rounded run step, even when
the selected observed cutoff moves earlier.

Default deferred tags are supplied. To use the LUT's declared date formats:

.. code-block:: python

   date_tags = DatasetPlanner.create_time_tags(
       template={"file_time_forecast_nwp": "%Y%m%d%H"},
       formats={"file_time_forecast_nwp": "time"},
   )
   # Supply time_tags=date_tags when constructing the planner.

An explicit ``time_tags`` dictionary replaces the defaults, so include every
deferred date tag used by your templates. The helper selects ``path_time_*`` /
``file_time_*`` fields declared as ``"time"``. Run tags reference ``time_run``;
destination and ``*_valid`` tags reference validity ``time``; other tags
reference ``source_time``. ``references={"custom_issue": "forecast_run"}``
selects a custom tag or overrides its reference. Ordinary time settings are
excluded. Both input arguments must be dictionaries and selected date templates
must be nonempty strings.

Check a window and adjust the observed cutoff
-----------------------------------------------

.. code-block:: python

   updated_period = planner.update_time(
       n_step_to_check_obs=3, n_step_to_check_frc=1,
       flag_adjust_obs_frc=True,
       rules_to_fill_obs="fill_using_frc", rules_to_fill_frc=None,
   )
   files = planner.file_plan
   available = planner.file_available()
   missing = planner.file_not_available()
   summary = planner.file_table("source")

The example checks only signed steps -3 through +1, including zero. A limit of
zero checks only zero; ``None`` disables that side. Both step limits default to
``None``, so calling ``update_time()`` checks no files. The older parameter names
``n_steps_observed`` and ``n_steps_forecast`` are accepted as aliases.

With adjustment enabled, missing required observations at the end of the checked
observed period are candidates for forecast replacement at the same validity
time. If steps -1 and zero are missing and -2 is available, those two steps can
switch to forecast. Each replacement requires every required forecast file for
every member. Missing optional variables do not trigger replacement; an earlier
internal gap does not move the cutoff when newer observations are available.
Incomplete replacements stay observed and unavailable.

The updated ``TimePeriod`` carries the new observed/forecast partitions, source
references and file IDs. The file plan is rebuilt with the selected source and
destination mappings, paths and members. ``time_checked``, ``time_available``
and ``time_adjusted`` explain the result. Unchecked availability remains null,
rather than false. Each call starts from the configured period with a fresh
unique-path cache, so observations arriving later can restore the original
selection. Only file presence is checked; no dataset timestamps or fields are read.

Set ``flag_adjust_obs_frc=False`` or ``rules_to_fill_obs=None`` for checks without
replacement. ``rules_to_fill_frc`` currently supports only ``None``. For an
explicit timestamp collection, use ``check_availability(check_times=[stamp])``;
``check_times`` is not a boolean. Omitting it checks the entire file plan.
``file_available`` and ``file_not_available`` return checked physical files and
exclude unknown statuses; pass ``"destination"`` for output-file reports.
Summary rows include ``time_mode`` as ``observed`` or ``forecast``.

Organize files by ensemble
----------------------------

Probabilistic sources use an inclusive ``ensemble`` range such as
``{"start": 1, "end": 10, "format": "03d"}`` and a member placeholder in
their path or filename. All probabilistic sources must use the same member IDs.
The default placeholders are ``ensemble_member``, ``enseble_member``, ``member``,
``member_id`` and ``ensemble_id``; pass ``ensemble_tags=("realization",)`` to
replace that list. Deterministic sources must not use member placeholders.

.. code-block:: python

   grouped = planner.file_by_ensemble()
   # With probabilistic sources:
   # {"001": {"2026-11-09 09:00:00": {"rain": "/data/.../rain.nc"}}, ...}
   outputs = planner.file_by_ensemble("destination", time_mode="forecast")
   forecast_sources = planner.file_by_ensemble("source", time_mode="forecast")

Shared deterministic fields appear under every member. Fully deterministic
plans use ``"deterministic"`` as the outer key. Time keys are validity timestamp
strings in chronological order; variable keys are canonical names. Grouping
uses the current adjusted plan and performs no additional file checks.
Use the same ``time_mode`` on both sides and look up destinations by the same
member, timestamp and variable keys; do not depend on two dictionary iteration
orders. ``None`` includes both modes. Forecast-filled observed gaps belong to
``forecast`` after adjustment.

Connect planning to runner execution
--------------------------------------

Dataset planning and model execution have separate configuration responsibilities.
Source/destination dictionaries describe forcing coverage. The runner's
``application_namelist`` selects the model/version, simulation interval, forcing
paths, restart and output fields. ``application_execution`` configures the
compiled binary, arguments, working copy and shared-library directories.
The planner does not automatically modify those execution sections.

After inspecting the plan, prepare or compose the actual forcing files, align
the namelist interval and ``sPathData_*`` paths with their published layout, then
use the deterministic or ensemble execution recipes in :doc:`runners`.
Keep namelists, executables, logs and restart/output paths separate per ensemble
member. Check model exit status and diagnostics after launch; file presence alone
does not establish a valid or successful simulation.

Availability history
--------------------

``update_time()`` supports ``flag_history=True``, ``reset_history=True`` and
``pickle_history="plans_file.workspace"``. The API refreshes by default.
Set ``reset_history=False`` to reuse a compatible saved plan without probing
source files again; changed configuration, run or window options rebuild it.
Missing or unreadable history also falls back to a fresh check, with log messages.
Set ``flag_history=False`` to disable history I/O entirely.

.. code-block:: python

   planner.update_time(
       n_step_to_check_obs=3, n_step_to_check_frc=1,
       flag_history=True, reset_history=False,
       pickle_history="/work/my_run/plans_file.workspace",
   )

A reused history contains an earlier availability snapshot. Use
``reset_history=True`` when new upstream files may have arrived. Load only your
own trusted pickle files. Output summaries always probe destinations afresh,
even when input planning reused history; see :doc:`dataset_summaries`.
