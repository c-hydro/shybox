Execution monitoring
====================

.. note::

   These integrations are included in SHYBOX 1.7.0 on main. Install the
   optional dependencies described below before using them.


The monitoring toolkit adapts HAT ``Analyzer_Execution`` run summaries to SHYBOX.
Explicit JSON lifecycle records replace execution-time inference from output
files. The Flask dashboard publishes these records at ``/monitoring``;
``/api/monitoring`` supplies the same summary as JSON.

Enable the runner
-----------------

Add ``monitoring`` inside the runner's ``application_execution`` object, beside
``description``, ``executable``, ``library`` and ``info``:

.. code-block:: json

   {
     "monitoring": {
       "enabled": true,
       "folder_name": "/absolute/path/to/shybox/exec/monitoring",
       "run_name": "nwp_icon_marche",
       "application": "hmc",
       "run_domain": "marche",
       "run_mode": "deterministic",
       "heartbeat_seconds": 30
     }
   }

``ExecutionManager`` reads these options automatically; its optional
``monitoring=...`` argument overrides the configuration. Monitoring is disabled
unless explicitly enabled. A missing ``folder_name`` defaults to a ``monitoring``
folder beside the existing execution info file. Writer paths support environment
variables and the runner's settings tags. Use an absolute shared folder when
workers and the dashboard have different working directories.

The existing execution info file, return value and skip rules remain supported.
A monitoring I/O error is logged without replacing the application's outcome.
Executable preparation errors, exceptions, subprocess timeouts and observed exit
codes are recorded. Skipped and dry-run attempts do not claim a process start or
exit code. As with the existing runner, nonempty stderr may fail execution even
when the process exits zero; the monitoring error and exit code show both facts.

Python applications and workers
--------------------------------

Use a monitor within each application or Dask worker function. Construct it on
the worker; do not share a live monitor between processes:

.. code-block:: python

   from shybox.monitoring_toolkit.execution import RunMonitor

   def execute_member(member, model_time):
       with RunMonitor(
           "/absolute/path/to/shybox/exec/monitoring",
           run_name="nwp_icon_marche", application="hmc",
           run_time=model_time, run_domain="marche", run_mode="probabilistic",
           ensemble=member, task_id=f"hmc-member-{member}",
       ):
           return compute_member(member)

Normal context completion records success; exceptions record failure and are
re-raised. For subprocesses, call ``start()`` followed by
``finish(process.returncode)``. A nonzero return code must be passed explicitly;
returning an integer from a Python function does not tell the context manager
whether the function failed. Dask is optional: the toolkit does not create a
cluster, submit tasks or connect to the scheduler dashboard.

Record contract
---------------

Each attempt/member receives a unique UUID filename. Records contain
``schema_version``, ``execution_id``, run/application identifiers, optional member
and task IDs, UTC initialization/start/update/finish timestamps, elapsed seconds,
status, exit code, host/PID, error and optional expected-output availability.
JSON files are replaced atomically. Heartbeats continue during quiet subprocess
execution and stop on completion.

Statuses are ``running``, ``succeeded``, ``failed``, ``cancelled``, ``skipped`` and
``dry_run``. The reader derives ``stale`` when a running heartbeat is older than
the configured threshold. Stale does not mean failed: power loss, SIGKILL or
worker loss may prevent the final write. Missing exit codes are never inferred
as zero. ``outputs`` accepts explicit filenames and records their availability
at completion separately from process success. It does not validate file content.

Dashboard and standalone summaries
----------------------------------

``hydrapp/config.json`` includes a ``monitoring`` section. Its ``folder`` must
match the writer folder. Reader paths are relative to the JSON configuration.
Declare applications by ``run_name`` and ``application``; optional ``run_domain``,
``run_mode`` and ``ensemble`` narrow the match. Undeclared executions are omitted.
A missing declared application remains visible even when no status file exists.

``expected_run_time`` optionally requires a particular UTC initialization;
otherwise the latest recorded initialization is selected. ``ensemble_members``
lists required members so a partially completed ensemble is not shown as fully
successful. Each member's newest attempt contributes to the application summary;
older attempts remain in history. Expected runs are declared explicitly, not
inferred from a scheduler. ``stale_seconds`` defaults to 180 and should exceed
the writer heartbeat interval.

The page refreshes every 30 seconds and supports application/date filters.
Dates refer to model initialization in UTC. If initialization is unavailable,
the date filter uses record update time. Malformed records are reported without
hiding valid results.

A standalone example is provided in
``workflow/monitoring/monitoring.example.json``:

.. code-block:: bash

   python -m shybox.monitoring_toolkit \
     --config workflow/monitoring/monitoring.example.json \
     --output /tmp/shybox-monitoring-summary.json

Legacy HAT references
---------------------

``inspect_legacy_run(start_file, output_files)`` reads reference-file modification
times and reports estimated elapsed time and output availability. A declared
application may supply:

.. code-block:: json

   {
     "legacy": {
       "start_file": "/archive/run/marche.info.txt",
       "output_files": ["/archive/run/hydrograph.json"]
     }
   }

When modern execution records are absent, the unfiltered application card shows
legacy availability with unknown process status. No exit code is inferred from
file presence. Legacy reference paths are also relative to the configuration.
Historical dates cannot be reconstructed from one pair of reference paths.

The old HAT source remains untouched. Discharge-threshold analysis continues in
the existing hydrological dashboard; HAT XML/CSV bulletin generation is not
ported by this execution-monitoring integration.

Process RAM and Ruler
----------------------

``ProcessMemoryMonitor`` samples per-process resident memory (RSS), virtual memory
(VMS), observed peak RSS and host RAM. Install ``psutil`` in the collector's
Python environment; it is listed in the base Conda environment and the optional
``monitoring`` package dependencies. ``--plot`` also uses Matplotlib.

.. code-block:: bash

   python -m workflow.monitoring.app_monitoring_memory_example \
     --config workflow/monitoring/memory_monitoring.example.json \
     --samples 12 --plot

The JSON contains ``memory_monitoring.interval_seconds``, ``processes`` and
``output.folder_name``. A target selects exactly one of ``pid`` (integer),
``name`` (exact process name) or ``cmdline_contains`` (case-sensitive substring).
``include_children`` includes descendants, deduplicated within each target.
Use ``--pid 12345`` to override the configured selectors with a known PID.
Name/command selectors can match multiple processes; explicit PIDs are tracked
with creation time so a reused PID is not mistaken for the original process.

``warning_rss_mib`` and ``critical_rss_mib`` classify measurements. Units are MiB
(1024 squared bytes). Summed process RSS can count shared pages repeatedly;
it is not a measurement of unique physical RAM. Missing processes report null
memory, not zero. Access failures and partial measurements remain visible.

The collector atomically replaces ``memory.latest.json`` and appends to daily
``memory_YYYY-MM-DD.csv`` files. Run one writer per output folder. Zero samples
means continuous polling until Ctrl+C. The optional PNG shows process RSS and
host RAM use, adapting the supplied HAT system-status plotting tool. The host
percentage uses total minus available RAM, separate from per-process RSS.

To enable the integrated Ruler collectors:

.. code-block:: bash

   python -m workflow.monitoring.app_monitoring_memory_example \
     --config workflow/monitoring/ruler_monitoring.example.json \
     --pid 12345 --samples 6 --plot

The example enables ``ruler.enabled``, ``ruler.disk_paths`` and
``ruler.process_memory_type`` (``partial`` or ``full``). The snapshot includes
Ruler's host/swap memory, disk, load and selected-process reports. Per-process
CPU percentages are point-in-time observations, not interval CPU profiling.
Full process memory may require additional permissions.

The toolkit includes the complete Python analysis/reporting implementation from
`c-hydro/ruler <https://github.com/c-hydro/ruler>`_ revision
``6fbbf6303620d1d9036a8176d405a3c66ae1d0bd``. Its MIT licence, copyright and
provenance are retained under ``monitoring_toolkit/resources/ruler``. Ruler process-control functions remain explicit APIs; the reporting wrapper
does not invoke them and rejects kill-enabled report configurations. The old file-arrival watchers are not ported
by this resource-monitoring integration.

Hydrapp reads ``monitoring.memory_file`` and optionally ``monitoring.memory_plot``
from its configuration; both are relative to that JSON file. Keep these outputs
in a separate resource folder from lifecycle JSON records. The demo configuration
points to ``hydrapp/demo/memory``. ``/api/monitoring/memory`` provides the latest
snapshot; the monitoring page refreshes it alongside lifecycle status.

Toolkit organization and full Ruler workflow
--------------------------------------------

Monitoring code is divided by responsibility:

.. code-block:: text

   shybox/monitoring_toolkit/
     common/              # Atomic JSON, UTC timestamps and configuration paths
     execution/           # Run lifecycle, expected runs and HAT execution summaries
     resources/           # Process/host RAM, CSV history, plots and snapshot readers
       ruler/
         handler.py       # Native Ruler report pipeline and workspace conversion
         adapter.py       # Ruler fields in SHYBOX resource snapshots
         engine/          # Complete package-integrated upstream Python implementation

Use topic imports in new applications:

.. code-block:: python

   from shybox.monitoring_toolkit.execution import RunMonitor, MonitoringCatalog
   from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor
   from shybox.monitoring_toolkit.resources.ruler import RulerMonitor, load_ruler_config

The former top-level imports remain compatibility facades. Execution status
files and resource reports remain independent: starting a Ruler analysis does
not imply that a hydrological model started or completed.

In addition to the lightweight snapshot collector, the native Ruler workflow
supports original JSON configurations, process sorting/filtering, scheduled
analysis, workspace chunks, CSV export and process/memory/disk report figures.
It retains the upstream conversion, logging and time utilities. Imports are
package-relative, avoiding collisions with other legacy ``lib_*`` modules.

Run a complete example with a bounded 32 MiB demo process:

.. code-block:: bash

   python -m workflow.monitoring.app_monitoring_ruler_demo \
     --output /tmp/shybox-ruler-demo

Or configure the real process name/command substring in
``workflow/monitoring/ruler_native.example.json`` and run:

.. code-block:: bash

   python -m workflow.monitoring.app_monitoring_ruler_example \
     --config workflow/monitoring/ruler_native.example.json

   # Equivalent package entry point:
   python -m shybox.monitoring_toolkit.resources.ruler \
     --config workflow/monitoring/ruler_native.example.json

Native ``process.analysis_time_frequency`` and ``analysis_time_period`` control
the sample cadence and duration. The native engine retains its minimum two-second
cadence and maximum one-hour analysis period. Report paths are relative to the
configuration; placeholders are filled by the Ruler engine. SHYBOX RAM collection
uses the separate ``memory_monitoring`` JSON schema and can select an exact PID.

For trusted existing workspace files, ``workspace_to_csv(files, destination)``
merges the report frames into an ordered CSV. Workspace files use Python pickle;
load only files produced by your own trusted workflow. Original workspace-export
and diagnostic command modules remain under ``resources.ruler.engine``.

Local integration changes preserve the MIT licence and are recorded in
``resources/ruler/SOURCE.md``: import isolation, process-exit races, direct psutil
host memory, UTC report sampling, current pandas indexing, multi-day time limits,
figure cleanup and configured CSV separators. The diagnostic loop is protected
by a main guard so importing its module does not start a background workload.
