Member summaries and model handoff
==================================

After all time steps of one member finish, call ``DatasetPlanner.summary()`` to
check that member's destination files afresh. It does not rebuild the time plan,
change forecast fallback decisions or reuse old destination availability.
The resulting JSON can be read by a separate model-running procedure without
loading dataset handlers or the original orchestrator.

Summarize the current member
----------------------------

.. code-block:: python

   summary_collections = {}
   sources = planner.file_by_ensemble('source', time_mode='forecast')
   for member, times in sources.items():
       execution = {}
       for time_key, variables in times.items():
           # Run the member/time orchestrator and collect scalar trace metadata.
           execution[time_key] = {'status': 'completed'}

       # Outside the time loop: this checks ONLY the current member.
       record = planner.summary(
           f'model_inputs.{member}.json',
           member=member, time_mode='forecast',
           extra_args=execution, tag_args='execution',
       )
       summary_collections[member] = record

The loop illustrates placement; replace its placeholder execution with actual
processing. An ensemble requires an explicit member such as ``001`` or ``002``.
For deterministic planning, omit ``member`` or pass ``'deterministic'``.
A single summary is a flat record containing ``"member": "001"``; it has no
outer member dictionary and contains no other member's files.

The sequential CLI does this after each completed member. With
``application.summary_merge=False`` (default), it writes
``datasets.summary.<time_run>.<member>.json`` beside the planning workspace.
``application.summary_file`` can replace the filename, with ``{member}`` or
``{ensemble_member}`` placeholders. An untemplated multi-member filename gains
a member suffix before the extension to prevent overwrites.

Manifest fields and readiness
-----------------------------

.. list-table:: Fields in each member record
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Meaning
   * - ``schema_version``, ``member``
     - Schema 1 and the exact member tag, including leading zeroes.
   * - ``time_run``, ``observed_run``, ``forecast_run``
     - Logical run time and selected observed/forecast issue cycles.
   * - ``time_start``, ``time_end``
     - Inclusive first/last planned forcing validity timestamps for the selection.
   * - ``time_period``, ``time_steps``
     - ISO validity timestamps and their count.
   * - ``time_frequency``, ``time_step_seconds``
     - Interval description and its duration in seconds.
   * - ``coverage``, ``time_mode``
     - Observed/forecast bounds and optional mode filter.
   * - ``steps``
     - Per-time mode, signed step, adjustment, readiness and variable file records.
   * - ``ready``, ``ready_steps``, ``missing_times``
     - Whether all selected destinations exist, complete step count and missing times.
   * - ``counts``
     - Unique expected/available/missing physical files and time-step counts.
   * - ``execution``
     - Validated caller metadata, or a custom field selected by ``tag_args``.

Each variable in ``steps`` includes ``output_name``, ``file_expected``,
``file_actual`` and ``exists``. Shared fields in one physical NetCDF file count
once in file totals. By default both a base path and its ``.gz`` counterpart are
checked; use ``allow_compressed=False`` for exact-path checks. An empty time
selection is not ready. ``time_mode=None`` includes both observations and
forecasts; use ``'observed'`` or ``'forecast'`` to select a group.

``verification='file_presence'`` means presence only. Readiness does not validate
variables, units, grid alignment, file contents or successful model execution.
Start/end remain planned bounds when files are missing; they are not automatically
shifted to the available subset. Planner timestamps are naive UTC ISO strings;
``generated_at`` and execution start/finish timestamps carry a UTC offset.

Merge completed records separately
----------------------------------

After the member loop, collect existing records without another scan:

.. code-block:: python

   merged = planner.summary_execution(
       summary_collections, file_name='model_inputs.json',
   )
   # merged['001']['member'] == '001'
   # merged['002']['member'] == '002'

The stored layout is ``{ '001': record_001, '002': record_002 }``. Every record
also retains its own ``member`` field. This method never probes files, creates
records for unfinished members or wraps the dictionary in global metadata.
Input dictionaries are copied and member keys must match each record's tag.

``application.summary_merge=True`` selects this behavior in the CLI: completed
member records remain in memory until the member loop ends, then one merged
file is written. The default is ``datasets.summary.<time_run>.json``. Merged
filenames cannot contain member placeholders. If execution stops early, the
final merged file is not published; separate mode preserves already completed
member files.

Extra metadata and JSON validation
----------------------------------

``summary()``, ``summary_execution()`` and ``write_summary()`` accept
``extra_args`` and ``tag_args='execution'``. Choose another nonempty tag such as
``'diagnostics'`` to store metadata in that field. Core names such as ``member``,
``time_run`` and ``ready`` are reserved and rejected as argument tags.

.. code-block:: python

   record = planner.summary(
       member='001', time_mode='forecast', tag_args='diagnostics',
       extra_args={
           'scenario': 'control', 'attempt': 1,
           'timing': {'duration_seconds': 2.75, 'completed': True},
       },
   )

Nested dictionaries are checked recursively. Keys must be strings; leaf values
may be strings, integers, finite floats or booleans. Invalid entries, lists,
``None``, objects, NaN/infinite values and circular dictionary references are
skipped with warnings identifying their location. Valid neighboring items are
preserved, including nesting, without modifying the caller's dictionary.
The default CLI stores its time-keyed execution trace under ``execution`` and
adds valid ``application.summary_extra_args`` there too.

Write and read existing summaries
---------------------------------

.. code-block:: python

   from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner

   planner.write_summary('copy.001.json', record)
   saved = DatasetPlanner.read_summary('copy.001.json')
   merged = DatasetPlanner.read_summary('model_inputs.json')

``write_summary(file_name, summary)`` writes existing records without rescanning.
It accepts one record or a member-keyed dictionary. By default it writes one
file per member, expanding placeholders or adding suffixes. Use
``flag_merge=True`` to write a member dictionary to one file; prefer the separate
``summary_execution()`` method when explicitly collecting completed members.
Each output is replaced atomically. JSON core-metadata and I/O errors raise;
an invalid extra argument alone does not prevent publication.

The static ``read_summary()`` needs no planner instance. It returns the stored
single or merged layout, checks schema version 1 and matching member tags/keys,
and neither probes destinations nor converts timestamp strings to datetimes.
A saved file is an earlier snapshot: recheck its paths before launching a model
if storage or upstream products may have changed.

Use the result in a model runner
--------------------------------

.. code-block:: python

   import pandas as pd
   from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner

   record = DatasetPlanner.read_summary('model_inputs.001.json')
   if not record['ready']:
       raise RuntimeError(f"Incomplete forcing for member {record['member']}")
   time_start = pd.Timestamp(record['time_start'])
   time_end = pd.Timestamp(record['time_end'])
   time_run = pd.Timestamp(record['time_run'])
   for step in record['steps']:
       rain_path = step['variables']['rain']['file_actual']
       # Supply actual paths to the model-input preparation stage.

For merged JSON, first select ``record = merged['001']``. Preserve the member
tag as a string. Align the namelist's forcing paths, simulation interval and
restart convention with the manifest; inclusive forcing bounds alone do not
set the model's restart timestamp. A model that expects plain NetCDF needs a
separate preparation step when ``file_actual`` ends in ``.gz``. Continue with
:doc:`runners` for HMC/S3M execution.
