# Dataset planning and workflow setup

`app_runner_workflow_hmc_datasets_main.py` plans observed/forecast files,
checks the configured window and executes forecast forcing sequentially by member
and time. It writes per-member JSON summaries after all times for that member.
Use `--dry-run --plan-files` for setup and a JSON plan without reading arrays,
writing forcing/summaries or reading/writing planning history. Preview can create logs.

Set `DOMAIN_NAME`, `PATH_GEO`, `PATH_OBS`, `PATH_FRC_NWP`, `PATH_DST`, `PATH_LOG`
and `PATH_FRC_RFARM` for probabilistic rain. From the repository root:

```bash
python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
  -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
  -time "2026-11-09 09:35" --dry-run --plan-files
```

Remove both planning flags to read inputs and write forcing. The same command
accepts the deterministic HMC and S3M profiles. Processing uses every forecast
step in the adjusted plan, not only the -3/+1 availability-check window.
Forecast workflows use `<VARIABLE>_WORKFLOW_FRC` when present, otherwise
`<VARIABLE>_WORKFLOW`; sources map raw fields to canonical names and destinations
map canonical names to model fields. Sample methods pass values through: adapt
interpolation, masks and units to your upstream data.

`configure_dataset_workflows(planner, configuration, time_mode='forecast')` is the
reusable setup-only API. It returns member/time dictionaries containing source,
destination and orchestrator handlers; invoke `.run()` explicitly. The CLI
returns scalar/path execution traces instead of storing these objects in JSON.

The CLI reuses compatible trusted planning history by default. Use
`application.planning.reset_history: true` to refresh availability or
`flag_history: false` to disable history. The default workspace is beside the
settings file; configure a run-specific `pickle_history` explicitly when needed.

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

In the main script, `summary_collections[member]` is populated once that member's
time iterations finish. `application.summary_merge` (default `false`) chooses
between individual files written at that point and a merged file written by
`summary_execution()` after all members finish. The main accepts
`application.summary_file` and `application.summary_extra_args`.

Default filenames beside the planning workspace are
`datasets.summary.<time_run>.<member>.json` for separate files and
`datasets.summary.<time_run>.json` for a merged file. Separate filename templates
can contain `{member}` or `{ensemble_member}`; the main adds a member suffix to
an untemplated multi-member filename to prevent overwrites. Merged filenames
cannot contain member placeholders.

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
