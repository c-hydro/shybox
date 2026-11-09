# Monitoring examples

From the SHYBOX repository root, run this with your SHYBOX Python environment:

```bash
python -m workflow.monitoring.app_monitoring_main_example \
  --run-time 2026-11-07T12:00:00Z
```

The example calls `monitor.start()` before the application and
`monitor.finish(exit_code=0)` after success. Exceptions call
`monitor.finish(exit_code=1, error=exc)` and return exit code 1 to the shell.
Ctrl+C records cancellation and returns 130. Heartbeats run automatically.
Replace `run_application()` with your own code.

This demonstration uses the dashboard's declared HMC identifiers; it does not
run Continuum. By default, its UUID status file goes to `exec/monitoring`, which
matches `hydrapp/config.json`. Open `/monitoring` in the running dashboard to
see it. Use `--folder-name /tmp/shybox-monitoring-demo` for isolated demo records.

Demonstrate a failure or an ensemble member:

```bash
python -m workflow.monitoring.app_monitoring_main_example --fail
python -m workflow.monitoring.app_monitoring_main_example --ensemble 1
```

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
process and writes a Ruler report and RAM figure. The default script output
folders match this demo configuration, not operational monitoring.

## Process RAM from JSON

```bash
python -m workflow.monitoring.app_monitoring_memory_example \
  --config workflow/monitoring/memory_monitoring.example.json --pid 12345 --samples 12 --plot
```

Replace `12345` with your process PID. Without `--pid`, the example selects all
commands containing `HMC_Model_V3_`. JSON also supports an exact `name` or a
`pid`, optional descendants and RSS warning/critical limits in MiB. Files go to
`exec/monitoring_ram`: atomic `memory.latest.json`, daily CSV and optional PNG.
Zero samples means continuous monitoring until Ctrl+C. Install `psutil` in the
collector environment; plotting requires Matplotlib.

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
