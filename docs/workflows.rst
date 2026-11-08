Workflow guide and status
=========================

Dataset workflows
-----------------

``workflow/dataset/convert/`` contains drivers for numerical-weather and
observed-data conversion, including IFS, ICON, station-to-grid and
grid-to-time-series applications.

``workflow/dataset/merge/`` combines data by domain or time. Select the HMC or
S3M variant that matches your file formats and model configuration.

``workflow/dataset/disaggregate/`` contains the RFarm ICON workflow. Its purpose
is to transform rainfall forecasts into stochastic ensemble members, with
intermediate results on disk and final model forcing written separately.

Model execution
---------------

``workflow/runner/`` contains HMC and S3M runners. These use utilities under
``shybox/runner_toolkit/`` to prepare namelists and launch model executables.
Supply an executable compatible with the selected namelist template, input
datasets and model version. Follow :doc:`runners` for complete HMC and S3M
configuration recipes, dry-run/launch commands, time conventions and ensemble
path isolation.

Geographic preparation and analysis
-----------------------------------

``workflow/geo/`` provides geographic preparation drivers, including catchment
processing. ``workflow/analyzer/`` contains model-output analysis drivers,
including time-series analysis.

Time-series JSON output
-----------------------

The HMC time-series analyzer can write the joined pandas result as NetCDF
or as Continuum hydrograph JSON. Select the destination format in
``application.dynamic_data_dst``. For one JSON file per section, use:

.. code-block:: json

   {
     "path": "{path_base_destination}/{path_time_destination}/",
     "file_name": "hydrograph_{section_tag}.json",
     "format": "json",
     "file_args": {
       "layout": "sections",
       "run_metadata": {
         "run_domain": "marche",
         "run_mode": "deterministic",
         "run_name": "nwp_ecmwf0100_realtime_marche",
         "run_var": "deterministic"
       },
       "var_mapping": {
         "discharge_sim_analysis": "time_series_discharge_simulated",
         "discharge_sim_openloop": "time_series_discharge_simulated_openloop",
         "discharge_obs": "time_series_discharge_observed",
         "air_temperature_obs": "time_series_air_temperature_observed"
       }
     }
   }

To write all sections into one JSON array instead, set ``layout`` to
``collections`` and use a filename without section placeholders, such as
``hmc.series_{domain_name}_{file_time_destination}.json``. The run and time
placeholders are resolved by the existing workflow; section placeholders are
resolved by the JSON wrapper. Existing NetCDF settings remain supported.

``shybox/io_toolkit/json_wrapper.py`` provides
``write_json_time_series_sections(data, file_path, ...)``. Its input is the
analyzer DataFrame with exactly two column levels: variable and section tag.
The section registry comes from ``data.attrs['data']`` or an explicit
``sections_db``/``attrs_data`` argument, and is matched by its unique ``tag``
field. The registry must be a pandas DataFrame; sections absent from it are
warned about and skipped. Missing section/variable combinations are exported as missing series.
The wrapper processes one section at a time and returns a tag-to-file manifest
for ``sections``, or the collection filename for ``collections``.

``shybox/io_toolkit/lib_io_json.py`` provides ``create_json_time_series`` to build one
section object and ``write_json_time_series_by_section`` to write it. These accept a
single-section DataFrame with a DatetimeIndex and variable columns. Metadata
and series are strings, following the legacy Continuum format. ``time_period``
uses ``YYYY-MM-DD HH:MM``; each ``time_series_*`` field is a comma-separated
string with exactly the same number of values. Timestamps must be sorted and
unique; changing ``time_format`` must not collapse distinct timestamps.

Use ``section_map`` to override output-field-to-registry-field mappings.
Defaults include basin, section name/code/id, drained area, baseflow, coordinates
and HMC indices when present. Alert maps to ``discharge_yellow`` or
``threshold_level_1``; alarm maps to ``discharge_orange`` or
``threshold_level_2``. ``section_idx_ji`` preserves the registry indices in
``j,i`` order, without changing their indexing convention. Missing registry
fields are omitted; retain extra fields in the input registry if needed.

``run_metadata`` supplies run labels and can explicitly set ``time_run``,
``time_restart`` and ``time_start``. Run timestamps are formatted as
``YYYYMMDDHHMM``. If not supplied, ``time_run`` uses the output call's reference
time. Restart and simulation-start times are not inferred from series bounds.

Without an explicit variable mapping, simulated analysis supplies the legacy
``time_series_discharge_simulated`` field when present; otherwise open-loop
discharge supplies it. When both exist, open-loop gets its own field. Use
``var_mapping`` to select the primary series explicitly; it takes precedence over
an orchestrator's variable mapping. Additional available rainfall or
soil-moisture variables can also be mapped; this writer does not calculate them.

NaN and infinity become ``data_no_value`` (default ``-9999.0``). Existing finite
sentinels such as ``-9998.0`` and ``-9999.0`` are preserved, as are negative air
temperatures. ``decimal_places`` optionally rounds finite values. Input data
is not changed. Files are replaced atomically; append is unsupported.

Per-section filenames require at least one of ``{section_tag}``, ``{section}``,
``{section_id}``, ``{section_code}``, ``{section_name}`` or ``{basin}``.
Only ``:``, ``!`` and ``|`` in filename values become underscores; collisions
are rejected before writing. For ``collections``, section placeholders trigger
a warning and use the literal value ``collections``. ``single`` remains an alias
for the collection layout.
Ensemble columns are rejected: the probabilistic analyzer/output schema is
a separate development step. The new Flask dashboard currently reads NetCDF;
connecting these JSON outputs to it requires a JSON reader.

Deployment
----------

Docker images and entrypoints are organized by task under ``docker/``.
Inspect each Dockerfile's repository and branch arguments: builds can fetch
a remote revision rather than using your local working-tree changes.
Apptainer conversion notes and HPC execution material are maintained separately.

Development limitations
-----------------------

This documentation edition is an initial guide, not a certification of every
example workflow. In the inspected development baseline:

* The RFarm adapter is a scaffold. Work on its model call, ancillary file
  collection and sequential output conversion is ongoing.
* The IFS converter imports ``exec_interpolate_data``, while the inspected grid
  adapter defines ``exec_interpolate_data_generic``. Check this interface before
  using that converter as a runnable example.
* Some example paths are specific to development machines or geographic domains.
* The test directory includes manual examples and fixtures; coverage of complete
  scientific workflows is limited.

For RFarm, storing one pickle per ensemble member avoids retaining the entire
ensemble in RAM. Reading a pickle still loads one full member, and the numerical
algorithm retains its own working arrays. Sequential file consumption and
ensemble-specific output paths are required for the final writing stage.
