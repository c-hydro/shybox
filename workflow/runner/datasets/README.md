# Dataset planning and execution

`app_runner_workflow_hmc_datasets_main.py` executes deterministic or probabilistic
forecast forcing sequentially by member and time. Adjusted observed gaps are
included in forecast execution. `app_runner_workflow_s3m_datasets_main.py` executes
both observed and forecast steps and accepts deterministic sources only.
Each runner saves a member summary after that member's time loop finishes.

Set `DOMAIN_NAME`, `PATH_GEO`, `PATH_OBS`, `PATH_FRC_NWP`, `PATH_DST`, `PATH_LOG`,
`PATH_TMP`, `PATH_PLANNING` and `PATH_SUMMARY`. HMC probabilistic rain also needs
`PATH_FRC_RFARM`. From the repository root:

```bash
python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
  -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
  -time "2026-11-09 10:00"

python -m workflow.runner.datasets.app_runner_workflow_s3m_datasets_main \
  -settings_file workflow/runner/datasets/app_runner_workflow_s3m_datasets_forcing.json \
  -time "2026-11-09 10:00"
```

These commands read inputs and write forcing files. The sequential entry points
accept `-settings_file` and `-time`. For metadata inspection, use `DatasetPlanner`
directly; the current scripts have no dry-run or setup-only helper.

Configuration uses `application.planning.options` for availability windows and
fallback rules, `planning.files` for history path/name and history flags, and
`planning.summary` for summary path/name. `update_time()` uses class options;
explicit arguments override them for that call. Set `reset_history: true` to
refresh availability, or `flag_history: false` to disable history I/O.
Only reuse trusted pickle workspaces. Shipped profiles refresh history by default.

`data_destination.collections` defines coordinates, field names, units and writer
arguments. Workflows use `<VARIABLE>_WORKFLOW_FRC` for forecasts and S3M additionally
uses `<VARIABLE>_WORKFLOW_OBS` for observations. S3M selects only the active mode's
workflows for each step. Every step writes all variables to one model forcing file.
Sample chains pass through values; adapt interpolation, masks and units to inputs.

See the [execution guide](../../../docs/dataset_execution.rst),
[planner guide](../../../docs/dataset_planning.rst),
[summary handoff](../../../docs/dataset_summaries.rst) and
[model execution](../../../docs/runners.rst) for complete instructions.
The experimental `datasets_parallel` folder is excluded from SHYBOX 1.7.1.

### Summarize one member after all its time steps

`DatasetPlanner.summary(member='001', ...)` checks ONLY that member's output
files and returns one flat record, including `"member": "001"`, run/source-cycle
times, forcing bounds, frequency, steps, paths, missing files, readiness and counts.
It does not loop over the other members. Call it after the member's time loop:

```python
summary_collections = {}
for member in members:
    for stamp in times:
        # Configure and run the orchestrator for this member/time.
        pass

    # Outside the time loop, still inside the member loop.
    summary_collections[member] = planner.summary(
        f'model_inputs.{member}.json', member=member, time_mode='forecast',
        extra_args={'scenario': 'control'},
    )
```

For a deterministic planner, `member` can be omitted; its record contains
`"member": "deterministic"`. A multi-member planner requires an explicit member.
No global metadata or `members` wrapper is added to this single record.

### Merge already completed member records

Use the separate `summary_execution()` method after the member loop:

```python
merged = planner.summary_execution(summary_collections, file_name='model_inputs.json')
```

This method returns and optionally writes a dictionary keyed by member:

```json
{
  "001": {"member": "001", "schema_version": 1, "time_run": "...", "steps": []},
  "002": {"member": "002", "schema_version": 1, "time_run": "...", "steps": []}
}
```

The abbreviated records omit other fields. Merging never reruns the planner,
rechecks files or creates summaries for missing members. It copies the supplied
records and validates that each key matches its record's member tag.

The runners call `summary(member=...)` after each member finishes and save the
configured `application.planning.summary` file. HMC summarizes forecast outputs;
S3M summarizes both observed and forecast outputs. Neither runner merges records
automatically; call `summary_execution()` explicitly when a merged handoff is needed.

`planner.file_planning` is the resolved workspace path after `update_time()`.
`planner.file_summary` is the latest saved member summary path, and
`planner.files_summary` maps each saved member to its path. Paths are absolute.
Summary filename templates support `{member}` and `{ensemble_member}`.

### Verification, extra arguments and summary I/O

Omit `time_mode` to include observations and forecasts, or select `'forecast'`
or `'observed'`. Summary generation does not rerun `update_time()` or change
fallback decisions or availability history. Start/end are inclusive planned
forcing validity bounds, even if files are missing. They are not model restart
settings. A model-running procedure should require the member record's
`ready == true` and use `steps` for each variable's `file_actual`.

Readiness checks file presence only, not NetCDF contents. Plain files and `.gz`
counterparts are accepted unless `allow_compressed=False`.

`extra_args` can contain nested dictionaries. Keys at every level must be
strings; leaf values can be strings, integers, finite floats or booleans.
Each dictionary is checked recursively, preserving allowed items and the
original nesting. Invalid entries and circular references are skipped with
warnings identifying their location. Lists, `None`, non-finite floats and
Python objects remain excluded. Input dictionaries are not modified.
Extra arguments stay inside the member record and cannot replace core fields.
Use `tag_args='execution'` (the default) to store them in `record['execution']`.
Set a different nonempty string, such as `tag_args='diagnostics'`, to choose
another field. The same option is supported by `summary_execution()` and
`write_summary()` when adding further arguments; existing custom fields are
preserved when simply writing or merging records. Core names such as `member`,
`time_run` and `ready` cannot be used as argument tags. The main explicitly
stores each member's time-keyed execution history under `execution`.

`write_summary(file_name, record)` atomically writes an already-created member
record without checking outputs. For low-level serialization it also accepts
`{member: record}`, with `flag_merge=True` to write that dictionary in one file.
Prefer `summary_execution()` for explicitly collecting completed member records.

`read_summary(file_name)` is static and needs no planner instance. It returns
the stored layout: one member record or a merged member-keyed dictionary. It
validates schema version 1 and matching member tags/keys without probing output
files or converting ISO strings to datetime objects. Core metadata and file
I/O errors are raised; invalid extra arguments are warned about and skipped on
creation/writing. Every individual output file is published atomically.
