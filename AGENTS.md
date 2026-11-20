# Repository guidance

## General

- Write source code, code comments, CLI text, README content, and documentation
  in English.
- Keep changes scoped to the requested component and preserve existing public
  APIs and configuration formats unless the task requires an explicit change.
- Follow the package layout under `shybox/`; keep workflow entry points and
  runnable examples under `workflow/`.
- In new SHYBOX applications, use the shared managers for logging, command-line
  arguments, and configuration. Import them from the reference modules below
  and use their existing methods for application setup.

  ```python
  from shybox.logging_toolkit.logging_handler import LoggingManager
  from shybox.config_toolkit.arguments_handler import ArgumentsManager
  from shybox.config_toolkit.config_handler import ConfigManager
  ```

  Keep logging setup, argument parsing, and configuration loading in these
  managers. Extend the shared implementation when an application needs behavior
  they do not yet provide.
- Add or update focused tests under `tests/` for behavior changes. Run them with
  the repository environment, for example:

  ```bash
  conda/envs/shybox_base_libraries/bin/python -B -m unittest tests.monitoring.test_resources
  ```

- Do not commit generated monitoring records, plots, local run outputs, or
  temporary workspace files.
- When declaring a new package version, update the README release summary,
  Read the Docs release/features pages, and `CHANGELOG` together. Date the
  changelog entry with the first update date for that version (`YYYY-MM-DD`);
  do not invent a release version for ordinary feature work.

## Monitoring

- Keep execution lifecycle records (`RunMonitor`) separate from resource
  snapshots (`ProcessMemoryMonitor`). They have separate schemas and consumers.
- Use an exact PID when the monitored application is known in-process. Process
  names and command-line substring selectors can match unrelated processes.
- Resource samples include process and host measurements. Process CPU readings
  use `psutil`'s interval-based percentage and may start at zero; retain the
  sampling interval and report units clearly.
- Preserve compatibility with `read_memory_snapshot` and Hydrapp when changing
  the resource snapshot schema. Update the corresponding reader, tests, and
  documentation together if the schema must change.
- Keep per-run identity (application name and execution ID) available when
  linking a lifecycle JSON record to resource CSV output.
