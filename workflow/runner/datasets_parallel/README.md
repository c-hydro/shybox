# Separate parallel dataset runner

All new code, tests and support files are in this folder. No existing workflow,
SHYBOX package method, configuration, test, or environment installer was changed.
This runner imports the existing SHYBOX library but never imports or executes
the existing sequential main script.

`reference_sequential_main.py.txt` is a read-only reference copy of the sequential
script at the start of this work. `original_files.sha256.json` records the existing
tracked source/configuration/documentation files, including their local edits.
Check that they are unchanged with:

```bash
python3 -B workflow/runner/datasets_parallel/verify_originals.py
```

## Preview with your current configuration

Run from the repository root, with the same input environment variables used by
the sequential runner (`DOMAIN_NAME`, `PATH_GEO`, `PATH_OBS`, `PATH_FRC_NWP`, and
`PATH_FRC_RFARM` when needed). Existing JSON files are read without modification.
The configured `PATH_DST`, log and temporary directories are overridden for this
variant: its files always belong to a new, isolated run directory.

```bash
./workflow/runner/datasets_parallel/run_datasets_parallel.sh \
  -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
  -time "2026-11-09 09:35" \
  --workers 2 --dry-run
```

For deterministic inputs, select
`app_runner_workflow_hmc_datasets_forcing_deterministic.json` instead.
The wrapper uses `conda/envs/shybox_geo_libraries/bin/python`; set `SHYBOX_PYTHON`
to use another existing interpreter. No packages are installed by these scripts.

By default the CLI only plans. It creates its own plan and log files, checks
filenames, and reads no scientific arrays or model outputs. Review `plan.json`
before actual processing. Add `--execute` to run workers:

```bash
./workflow/runner/datasets_parallel/run_datasets_parallel.sh \
  -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
  -time "2026-11-09 09:35" \
  --workers 2 --task-unit member --execute
```

Each invocation defaults to a unique `runs/<timestamp>-<id>/` in this folder.
`--run-dir /absolute/path/to/a/new/run` chooses another location. An existing run
directory is rejected to prevent overwriting an earlier run. Preview and execution
use separate new directories; there is no automatic resume or in-place retry.

The existing sequential command and its original destination paths remain the
way to run the current procedure. This variant does not replace that entry point.

## Work partitioning

- `--task-unit auto`: one job per probabilistic member, covering its times in
  order; independent time-step jobs for a deterministic run.
- `--task-unit member`: one job per member with sequential time steps. A fully
  deterministic run therefore has one job, even with several workers available.
- `--task-unit time`: independent jobs for member/time combinations.
- `--time-chunk 4`: in time mode, group four adjacent times for the same member
  into one job to reduce setup overhead.
- `--time-mode forecast` (default), `observed`, or `all`: select which validity
  times to prepare. Adjusted observed gaps belong to forecast, as in the planner.

Workflow names can be `<VARIABLE>_WORKFLOW_FRC` for forecast,
`<VARIABLE>_WORKFLOW_OBS` for observations, or `<VARIABLE>_WORKFLOW` for either.
Requesting a mode without a matching process list fails during planning.

Time jobs require independent time-step processing. Choose member jobs when
processing depends on the previous step. Every job processes **all variables**
for its member/time and owns the complete destination file; variables are never
assigned to competing writers of that same file.

Rainfall can vary by member while temperature, radiation, humidity and wind
come from shared deterministic sources. Each complete output still includes
all configured variables. This first variant parallelizes the work; it does
not yet compute/cache deterministic processing once globally across all members.
Start with two workers and measure RAM, disk throughput and run duration before
increasing concurrency. Small jobs may be slower because of process startup.

## Isolation and worker behavior

- Workers use separate spawned Python processes. No `DataLocal`, orchestrator,
  xarray object, logger or open NetCDF handle is passed between processes.
- Only configuration and planned filenames are transferred to workers. Handlers,
  process registration and orchestrators are constructed locally in each worker.
- Each task gets its own log and temporary directory. Compressed sources are
  decompressed privately there; GRIB sources are copied there so generated indexes
  cannot interfere with shared inputs. Plain NetCDF sources are read in place.
- Source files are not deleted by the new code. Output paths are rewritten under
  the new run directory, and preflight rejects escaping paths, input/output aliases
  and output files shared by different member/time jobs.
- Planning is done once in the parent. It never reads or overwrites the existing
  sequential `.workspace` file; history reuse is disabled for this variant.
- At most twice the worker count is submitted at once. `--native-threads 1`
  limits inner numerical-library thread pools in workers to avoid oversubscription.
- Errors are reported per task. A failed member does not stop independent jobs.
  A crashed process pool marks remaining jobs failed; it does not silently retry.
- Existing SHYBOX readers/writers are reused. They may produce `.nc` or `.nc.gz`;
  summaries report the actual path found. This folder does not patch their behavior.

The selected workflow's scientific methods and optional dependencies are the
same ones used by the existing package. Test custom methods with this variant
before using them operationally. Output file ownership is enforced by the runner;
custom methods with their own unrelated file paths still control those paths.

## Reports and member summaries

Each run contains:

```text
plan.json                       # jobs, members, times, isolated output paths
forcing/                        # complete forcing files
logs/planner.log
logs/member_<member>_<job>.log
reports/member_<member>_<job>.json
summaries/member.<member>.json
summaries/all_members.json
execution.json                  # task status, errors and manifest paths
tmp/                            # private workspaces and staged sources
```

The parent collects only trace metadata. It calls `planner.summary(member=...)`
once all jobs for that member finish, then calls `summary_execution()` to merge
completed member records. Each record contains its member tag, times, file
availability and `execution` traces. No orchestration or data objects are saved.
`ready` is a file-presence check, not validation of NetCDF contents; `execution.json`
reports failure if either processing failed or expected outputs are missing.
The CLI exits with status 1 on a failed execution.

## Python API and tests

```python
from workflow.runner.datasets_parallel.pipeline import (
    load_settings, prepare_run, run_parallel_datasets,
)

application, workflow, tags = load_settings("settings.json")
prepared = prepare_run(
    application, workflow, time_tags=tags, time_run="2026-11-09 09:35",
    run_dir="/tmp/a_new_parallel_run", task_unit="auto",
)
print(prepared.report())
# Put actual process launching under if __name__ == '__main__': in a script.
report = run_parallel_datasets(prepared, max_workers=2)
```

Run the tests using the scientific environment; all tests are in this folder:

```bash
conda/envs/shybox_base_libraries/bin/python -B -m unittest discover \
  -s workflow/runner/datasets_parallel -p test_parallel_datasets.py -v
```

Tests cover real spawned processes, worker exceptions/crashes, deterministic and
probabilistic NetCDF outputs containing all variables, private gzip staging,
missing member inputs, existing-profile dry runs, output ownership and unchanged
original files. Tests create data and outputs only under temporary directories.
