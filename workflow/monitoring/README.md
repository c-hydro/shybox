# Monitoring examples

From the SHYBOX repository root, run this with your SHYBOX Python environment:

```bash
python -m workflow.monitoring.app_monitoring_main_example \
  -settings_file workflow/monitoring/app_monitoring.example_run.json \
  -time "2026-11-07 12:00"
```

Pass the settings file with `-settings_file`. Edit `configuration.monitoring`
to change the output path, run identity, heartbeat interval, or demo workload.
Relative paths are resolved from the working directory. `-time` overrides
`run_time`, and a `null` `run_time` uses the current UTC time. For an ensemble
record, set `run_mode` and `ensemble` in the JSON.

`configuration.monitoring.path` and `file_name` support placeholders. For
example, `monitoring/{model}` and
`{model}_{time_run:%Y%m%d%H%M}_{execution_id}.json` organize records by model
and initialization time. `model` defaults to `application`, and `time_run`
and `run_time` default to the run timestamp. Set `placeholders` to supply
values such as `{"model": "zzo"}`. Other monitoring settings can be reused as
`{run_name}` or `{run_domain}`. Nested custom values such as
`{"site": {"code": "east"}}` in `placeholders` can be used as `{site.code}` or
`{placeholders.site.code}`. Unresolved placeholders are logged and stay in the
resulting path.

`time_rounding` defaults to `"floor"` (also accepted as `"flat"`). Use `"ceil"`
to round up to the next UTC hour or `null` to keep the original precision.
Rounding applies to the run time in the record and to `time_run` and `run_time`
placeholders. An exact hour stays unchanged with either rounding mode.

If `time_format` is omitted, bare `{time_run}` and `{run_time}` placeholders
use `%Y%m%d%H%M`. Set `time_format` in JSON to change that default or use an
inline format such as `{run_time:%Y-%m-%d}` for a specific placeholder. The
JSON record itself stores the run time in ISO format.

`configuration.log` is optional. Missing `path` and `file_name` default to
`shybox/logs` under the system temporary directory and
`app_monitoring_main_example.log`. The example logs to the console by default.
Set `handlers` to `["stream", "file"]` to also write a log file.

The `MonitoringExecutionManager` context starts the record and heartbeats,
then saves the final exit code. Success returns 0, an unhandled exception
returns 1, and Ctrl+C records cancellation and returns 130. Replace
`run_application()` with your own code.

This demonstration uses the dashboard's declared HMC identifiers. To view the
record in Hydrapp, set `path` to `exec/monitoring` from the repository root and
open `/monitoring` in the running dashboard. Use a path such as
`/tmp/shybox-monitoring-demo` for isolated demo records. Set `fail` to `true`
to demonstrate a failed execution.

For a subprocess, pass its actual return code to `monitor.finish()`; the success
code in this example applies to a Python application that completed normally.

Create a standalone summary using the existing reader configuration:

```bash
python -m shybox.monitoring_toolkit \
  --config workflow/monitoring/monitoring.example.json \
  --output /tmp/shybox-monitoring-summary.json
```

See [the monitoring guide](../../docs/monitoring.rst) for the record format,
runner configuration and legacy HAT reference files.

## Run-status demo for Hydrapp

```bash
python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources
```

Start Hydrapp with `--config hydrapp/config.demo.json`. The demo covers success,
failure, skipped, dry-run, stale, missing and incomplete ensemble runs; the
analyzer heartbeat is live until the script exits. `--resources` samples the demo
process every two seconds and writes a Ruler report, a CPU/RAM resource snapshot,
a daily CSV and RAM figure. The CSV includes process and host CPU percentages
alongside memory statistics. The default script output folders match this demo
configuration, not operational monitoring.

## Process RAM from JSON

```bash
python -m workflow.monitoring.app_monitoring_memory_example \
  --config workflow/monitoring/memory_monitoring.example.json --pid 12345 --samples 12 --plot
```

Replace `12345` with your process PID. Without `--pid`, the example selects all
commands containing `HMC_Model_V3_`. JSON also supports an exact `name` or a
`pid`, optional descendants and RSS warning/critical limits in MiB. Files go to
`exec/monitoring_ram`: atomic `memory.latest.json`, daily CSV and optional PNG.
The CSV includes process and host CPU percentages as well as RSS/VMS and host
memory. Zero samples means continuous monitoring until Ctrl+C. Install `psutil`
in the collector environment; plotting requires Matplotlib.

For Ruler host/swap memory, disk, load and process reports, use the same command
with `--config workflow/monitoring/ruler_monitoring.example.json`. The adapted
collectors are pinned to the upstream revision documented in
`shybox/monitoring_toolkit/resources/ruler/SOURCE.md` with its MIT licence.

To observe the live status demo by command instead of PID, use
`memory_monitoring.demo.json`; it writes to `hydrapp/demo/memory`, which the
Hydrapp demo page reads. Run one collector per output folder. Process RSS may
count shared pages repeatedly; it is distinct from host RAM usage.

## Toolkit topics and complete native Ruler reporting

New code imports lifecycle helpers from `shybox.monitoring_toolkit.execution`,
RAM/host utilities from `shybox.monitoring_toolkit.resources`, and full Ruler
reporting from `shybox.monitoring_toolkit.resources.ruler`. JSON/time/path helpers
live in `common`. Previous top-level imports remain supported.

For a standalone example generating Ruler workspace chunks, a combined CSV and
process/memory/disk PNG figures:

```bash
python -m workflow.monitoring.app_monitoring_ruler_demo --output /tmp/shybox-ruler-demo
```

The script starts a bounded 32 MiB demo process and cleans up that process on
completion. It does not operate on hydrological model processes.

To monitor an operational process using the full original Ruler JSON schema,
edit `process.name`, timing and output paths in `ruler_native.example.json`:

```bash
python -m workflow.monitoring.app_monitoring_ruler_example \
  --config workflow/monitoring/ruler_native.example.json
```

The equivalent package command is `python -m shybox.monitoring_toolkit.resources.ruler`.
This is separate from `ruler_monitoring.example.json`, which configures Ruler
fields within the SHYBOX RAM snapshot sampler. Native process-control functions
are available explicitly in the engine; the reporting wrapper never runs them.
