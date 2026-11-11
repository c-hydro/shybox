Running HMC and S3M
===================

The runners create a model-specific Fortran namelist, prepare an executable,
launch it and collect execution information. They do not download a model binary,
build geographic datasets or convert forcing data. Prepare those inputs first,
using :doc:`installation`, :doc:`quickstart` and :doc:`datasets`.

For the scientific model presentations, see :doc:`appendix_hmc` (Appendix A)
and :doc:`appendix_s3m` (Appendix B). This chapter covers their configuration
and execution through SHYBOX.

This guide uses the current base runners for one deterministic run. Their JSON
examples need adaptation to your domain and executable. Commands below are run
from the SHYBOX repository root.

.. note::

   The base runner interfaces are available in the published repository. These
   recipes were checked against the development checkout, which also contains
   pending namelist and execution-status fixes. Check the generated namelist
   and model exit status when using an older revision. The optional monitoring
   integration below is a development preview until its code is published.

Dataset planning and model execution
------------------------------------

Configure forcing coverage before selecting the model interval and launching an
executable. ``DatasetPlanner`` is available on ``main`` and uses separate
``info``, ``time``, ``data_source`` and ``data_destination`` dictionaries.
It computes observed/forecast paths, checks only the requested signed time
window and can replace a trailing observed gap with complete forecast files.
Its tables use ``observed``/``forecast`` modes and include source cycles,
availability and ensemble members.

.. toctree::
   :maxdepth: 2

   dataset_planning

The :doc:`dataset_planning` guide provides a complete configuration and Python
example, deferred tags, ``dims="2d"`` / ``"3d"`` source references, window
limits, forecast fallback, member-organized file dictionaries and per-variable
``DataLocal`` / ``OrchestratorConfig`` setup in the dataset entry point. The separate
:doc:`datasets` chapter covers the development forcing-composition workflow.

For model execution, ``application_namelist`` configures the model/version,
forcing paths, simulation interval, restart and output fields;
``application_execution`` configures the binary, arguments, working copy and
shared libraries. Planning does not modify either execution section or create
forcing data. Prepare the planned files, align their interval and paths with
the namelist, then use the model-specific recipes below. Isolate working and
output paths per ensemble member.

Choose the runner and version
-----------------------------

.. list-table:: Base runner examples
   :header-rows: 1
   :widths: 16 16 34 34

   * - Model
     - Template version
     - Python driver in ``workflow/runner/execution/``
     - JSON example in ``workflow/runner/execution/``
   * - HMC / Continuum
     - 3.3.0
     - ``app_runner_workflow_hmc_base_main.py``
     - ``app_runner_workflow_hmc_base_example_v330.json``
   * - HMC / Continuum
     - 3.1.6
     - Same HMC base driver
     - ``app_runner_workflow_hmc_base_example_v316.json``
   * - S3M
     - 5.3.3
     - ``app_runner_workflow_s3m_base_main.py``
     - ``app_runner_workflow_s3m_base_example_v533.json``

``NamelistTemplateManager`` currently registers HMC 3.1.6, 3.2.0 and 3.3.0, and
S3M 5.3.3. Select the model using ``application_namelist.description.type`` and
``version``. The executable must support that namelist version. Changing the
``file.template`` label alone does not register a new template; the active
structures live in ``shybox/runner_toolkit/namelist/namelist_template_*.py``.

Prepare the environment
-----------------------

Activate the scientific environment and expose the repository to Python:

.. code-block:: bash

   source ./conda/shybox_base_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
   python --version

Use the scientific Python environment, rather than the smaller Hydrapp viewer
environment. Supply a compiled HMC/S3M binary and the shared libraries against
which it was built. ``application_execution.library.dependencies`` contains
library directories; SHYBOX adds them to the child process's library search path.
An empty list is appropriate only when the binary already finds its libraries.

The three important application sections are:

.. list-table:: Configuration responsibilities
   :header-rows: 1
   :widths: 28 72

   * - Section
     - Purpose
   * - ``configuration.variables``
     - Logical values, environment mappings, types and formatting templates.
   * - ``application_namelist``
     - Model/version, namelist location, explicit parameters and pattern updates.
   * - ``application_execution``
     - Executable source, working copy, arguments, library directories and info file.

``library.location`` is the source binary. ``executable.location`` is the working
copy launched from its parent directory. Keep those paths different: preparing
an updated execution can replace the working copy. ``executable.arguments``
passes the generated namelist filename to the model.

HMC: configure one deterministic run
------------------------------------

Copy the HMC 3.3.0 example and define your paths. Replace the source and binary
paths below with existing files on your machine:

.. code-block:: bash

   export SHYBOX_RUN_ROOT="$HOME/shybox_runs/hmc"
   export DOMAIN_NAME="my_domain"
   export PATH_APP="$SHYBOX_RUN_ROOT/exec"
   export PATH_NAMELIST="$PATH_APP"
   export PATH_SRC="/absolute/path/to/hmc_inputs"
   export PATH_DST="$SHYBOX_RUN_ROOT/results"
   export PATH_LOG="$SHYBOX_RUN_ROOT/log"
   export HMC_BINARY="/absolute/path/to/HMC_Model_V3_3_0.x"
   export TIME_RUN="2026-11-08 00:00"
   export TIME_RESTART="2026-11-07 23:00"
   export TIME_PERIOD="24"

   mkdir -p "$PATH_APP" "$PATH_DST" "$PATH_LOG"
   cp workflow/runner/execution/app_runner_workflow_hmc_base_example_v330.json \
      "$SHYBOX_RUN_ROOT/settings.json"

The base HMC example expects geographic and forcing subfolders under
``PATH_SRC``. Adapt its ``sPathData_*`` fields to your actual layout:

.. code-block:: text

   hmc_inputs/
     data_geo/gridded/
     data_geo/point/
     data_forcing/gridded/YYYY/MM/DD/
     data_forcing/point/YYYY/MM/DD/
     data_forcing/time_series/

Only the inputs selected by your namelist's model flags are required by the
Fortran model. Their filenames, units and spatial grids must match that build.
Review restart paths separately: the example's read-restart and write-state
folders are not identical.

Apply these setup edits to the copied JSON. This also adds the top-level
``info`` section consumed by the current HMC base driver, which is absent from
its original base example:

.. code-block:: bash

   python - "$SHYBOX_RUN_ROOT/settings.json" <<'PY'
   import json
   from pathlib import Path
   import sys

   path = Path(sys.argv[1])
   cfg = json.loads(path.read_text())
   environment = cfg["configuration"]["variables"]["lut"]["environment"]
   environment["path_log"] = "PATH_LOG"
   execution = cfg["application_execution"]
   execution["library"]["location"] = "$HMC_BINARY"
   execution["library"]["dependencies"] = []
   execution["info"]["location"] = "{path_app}/execution_hmc.info"
   execution["description"]["dry_run"] = True
   cfg["info"] = {
       "path": "{path_app}/reports",
       "file_name": "runner_hmc.json",
       "format": "json"
   }
   path.write_text(json.dumps(cfg, indent=2) + "\n")
   PY

Set any required library directories in ``library.dependencies`` before running.
Review the domain parameters in ``application_namelist.fields.by_value``:
``dUc``, ``dUh``, ``dCt``, ``dCf``, ``dCN`` and other calibration/physics flags
are example values, not a calibration for your basin. Set ``iFlagRestart`` to
match your intended initialization; a restart run needs valid model state files.

Perform a dry run
~~~~~~~~~~~~~~~~~

With ``description.dry_run`` set to true in the copied HMC JSON:

.. code-block:: bash

   python -c 'from workflow.runner.execution.app_runner_workflow_hmc_base_main import main; main(view_table=False, dry_run=True)' \
      -settings_file "$SHYBOX_RUN_ROOT/settings.json"

This generates the namelist and prepares/checks the executable without launching
the model. It can create or replace local files. Check the generated
``exec_base_my_domain.namelist``, the log and ``exec/reports/runner_hmc.json``.
Dry-run preparation errors are recorded in the checks; a completed Python
command alone does not establish that all dependencies or model inputs exist.

Launch HMC
~~~~~~~~~~~

Once the generated namelist and input datasets have been checked, change
``application_execution.description.dry_run`` to false in the copied JSON, then:

.. code-block:: bash

   python -c 'from workflow.runner.execution.app_runner_workflow_hmc_base_main import main; main(view_table=False, dry_run=False)' \
      -settings_file "$SHYBOX_RUN_ROOT/settings.json"

The HMC JSON value overrides the ``main()`` argument. Keep them consistent.
Model stdout/stderr is streamed to the runner log. The generated working binary,
namelist and execution info are under ``PATH_APP``; model outputs follow the
``sPathData_Output_*`` and ``sPathData_State_*`` fields in the namelist.

S3M: configure one snow-model run
---------------------------------

Copy the S3M 5.3.3 example into a separate working directory:

.. code-block:: bash

   export SHYBOX_RUN_ROOT="$HOME/shybox_runs/s3m"
   export DOMAIN_NAME="my_snow_domain"
   export PATH_APP="$SHYBOX_RUN_ROOT/exec"
   export PATH_NAMELIST="$PATH_APP"
   export PATH_STATIC="/absolute/path/to/s3m_static_grids"
   export PATH_SRC="/absolute/path/to/s3m_forcing"
   export PATH_DST="$SHYBOX_RUN_ROOT/results"
   export PATH_LOG="$SHYBOX_RUN_ROOT/log"
   export S3M_BINARY="/absolute/path/to/S3M_Model_V5_3_3.x"
   export TIME_RUN="2026-11-08 00:00"
   export TIME_RESTART="2026-11-06 23:00"
   export TIME_PERIOD="24"

   mkdir -p "$PATH_APP" "$PATH_DST" "$PATH_LOG"
   cp workflow/runner/execution/app_runner_workflow_s3m_base_example_v533.json \
      "$SHYBOX_RUN_ROOT/settings.json"

``PATH_STATIC`` supplies the S3M geographic grids. The example uses dated
``input/`` folders under ``PATH_SRC``, with optional ``updating/`` and
``updating_swe/`` datasets when the corresponding assimilation flags are active.
Prepare forcing in the format selected by the S3M namelist.

Apply these edits to select your binary and separate output/state paths from
the forcing folder:

.. code-block:: bash

   python - "$SHYBOX_RUN_ROOT/settings.json" <<'PY'
   import json
   from pathlib import Path
   import sys

   path = Path(sys.argv[1])
   cfg = json.loads(path.read_text())
   execution = cfg["application_execution"]
   execution["library"]["location"] = "$S3M_BINARY"
   execution["library"]["dependencies"] = []
   execution["info"]["location"] = "{path_app}/execution_s3m.info"
   fields = cfg["application_namelist"]["fields"]
   fields["by_value"]["iFlagRestart"] = 0
   fields["by_value"]["sPathData_Output_Gridded"] = "{path_destination}/output/$yyyy/$mm/$dd/"
   fields["by_value"]["sPathData_Restart_Gridded"] = "{path_destination}/state/$yyyy/$mm/$dd/"
   fields["by_pattern"]["pattern_path_data"]["active"] = False
   cfg["log"]["file_name"] = "runner_s3m.txt"
   path.write_text(json.dumps(cfg, indent=2) + "\n")
   PY

Set required shared-library directories in ``library.dependencies``. Before a
real model run, update these S3M-specific parameters:

.. list-table:: S3M domain and initialization settings
   :header-rows: 1
   :widths: 32 68

   * - Parameter
     - What to check
   * - ``a1dGeoForcing``
     - Geographic origin of the forcing grid, in the order expected by S3M.
   * - ``a1dResForcing``
     - Spatial resolution of that grid, consistent with its coordinate units.
   * - ``a1iDimsForcing``
     - Grid dimensions in the order expected by the selected model build.
   * - ``iFlagRestart``
     - The recipe selects a cold start (0). Use 1 only with compatible restart files at the expected time/path.
   * - ``iFlagSnowAssim`` and related assimilation flags
     - Enable only when the corresponding updating/SWE datasets are supplied.
   * - Snow/ice parameters
     - Review the ``S3M_Snow`` defaults and model flags for your domain and experiment.

The geographic values in the shipped example belong to its example domain;
changing only ``DOMAIN_NAME`` does not make those values correct elsewhere.

Check and launch S3M
~~~~~~~~~~~~~~~~~~~~

Generate and inspect the namelist before running the model:

.. code-block:: bash

   python -c 'from workflow.runner.execution.app_runner_workflow_s3m_base_main import main; main(view_table=False, dry_run=True)' \
      -settings_file "$SHYBOX_RUN_ROOT/settings.json"

After validation, launch the executable:

.. code-block:: bash

   python -c 'from workflow.runner.execution.app_runner_workflow_s3m_base_main import main; main(view_table=False, dry_run=False)' \
      -settings_file "$SHYBOX_RUN_ROOT/settings.json"

The S3M base driver passes its ``main(dry_run=...)`` argument to the execution
manager; it does not read a JSON ``description.dry_run`` override. Running the
S3M file directly currently calls ``main(..., dry_run=True)``. The commands above
make the execution choice explicit without editing the driver.

Time, placeholders and parameter precedence
--------------------------------------------

The current base drivers parse ``-time`` but do not forward it into their time
configuration. Set ``TIME_RUN`` through the environment/LUT as shown above;
do not rely on ``-time`` as an override. There is no dedicated ``--dry-run``
command-line option in these drivers.

With the current hourly examples:

* HMC uses ``time_priority='run'``: its start is ``TIME_RUN`` and its end is
  computed forward from ``TIME_PERIOD``.
* S3M uses ``time_priority='period'``: its end is ``TIME_RUN`` and its start is
  computed backward by the configured period.
* ``sTimeStart``, ``sTimeRestart`` and ``iSimLength`` are resolved into the
  namelist. Set ``TIME_RESTART`` to match the state file required by your
  initialization; it does not move the simulation start/end. Restart values
  are resolved separately through the LUT, so inspect ``sTimeRestart`` rather
  than assuming it is automatically consistent with the interval.

For example, with ``TIME_RUN="2026-11-08 00:00"`` and a 24-hour period, HMC
starts on 8 November; S3M starts on 7 November. Ensure forcing/state coverage
matches the generated model interval. These drivers currently use TimeManager's
``Europe/Rome`` default timezone. Keep model, forcing and restart time conventions
consistent; do not assume that a timestamp without an offset is interpreted as
UTC. Check the generated compact model timestamps before execution.

SHYBOX resolves ``{path_source}``, ``{time_start}`` and similar settings. Literal
``$yyyy/$mm/$dd`` tokens in model paths are left for the model's date substitution;
keep them quoted when editing settings from a shell.

``fields.by_value`` sets individual parameters. Active ``fields.by_pattern``
entries set groups of names matching a substring. Explicit ``by_value`` entries
win when both match. The examples use an ``iDt`` pattern with 3600-second values;
adapt model/forcing/output intervals together for another cadence.

The runners validate mandatory namelist fields, but this does not check that
all forcing dates, static grids, restart contents or scientific parameter values
are correct. Inspect the model's own diagnostic output as well.

Execution info and live monitoring
----------------------------------

``application_execution.info.location`` is the serialized execution-result file
used by ``ExecutionManager``. It is distinct from the HMC top-level ``info``
section, which exports a readable execution summary, and from model restart
files. The current base drivers hard-code ``execution_update=True`` and namelist
``overwrite=True``; changing only ``configuration.flags.update_execution`` does
not enable reuse. Direct API users can choose ``execution_update=False`` when
an existing execution result should be reused.

To publish live start/heartbeat/end records, add ``monitoring`` inside
``application_execution``:

.. code-block:: json

   {
     "enabled": true,
     "folder_name": "/absolute/path/to/shared/monitoring",
     "run_name": "my_experiment",
     "application": "hmc",
     "run_domain": "my_domain",
     "heartbeat_seconds": 30
   }

Use ``"application": "s3m"`` for snow runs. Declare the same run/application
identifiers in Hydrapp's monitoring configuration and point it to the same folder.
See :doc:`monitoring` for lifecycle records, RAM/Ruler collection and examples.
A successful execution or available output does not establish predictive skill.

HMC ensembles and orchestration
-------------------------------

The experimental HMC orchestrator is
``app_runner_workflow_hmc_orchestrator_main.py``. Its paired JSONs describe
deterministic and probabilistic configurations. For a single deterministic run,
the base recipe above is the recommended starting point: the specialized
orchestrator callback currently assumes ``ensemble_member`` tags that the generic
deterministic ``RunSpec`` does not supply.

For ensembles, ``orchestration`` selects member identifiers and scheduling:

.. code-block:: json

   {
     "simulation_mode": "probabilistic",
     "execution_strategy": "sequential",
     "members": {"start": 1, "end": 5, "step": 1, "format": "{member:03d}"},
     "max_workers": 1,
     "max_retries": 0,
     "continue_on_error": false
   }

The shipped probabilistic example currently selects only member 001; change the
range explicitly. Its paths use ``run_{ensemble_member}`` for member forcing and
results. Keep executable working directories, namelists, execution info, logs,
restart reads and written states isolated by member. For example:

.. code-block:: json

   {
     "namelist_location": "{path_app}/{ensemble_member}/{file_namelist}",
     "executable_location": "{path_app}/{ensemble_member}/HMC_Model_V3_{execution_name}.x",
     "arguments": "{path_app}/{ensemble_member}/{file_namelist}"
   }

These are path patterns: place them in ``application_namelist.file.location``
and the corresponding ``application_execution.executable`` fields. Begin with
one member and sequential execution; introduce ``parallel`` and multiple workers
only after member paths and resource requirements have been verified. The model
callback must reject failed subprocesses so retries and summaries are meaningful.
S3M's supplied base workflow is a single-run driver, not an S3M ensemble workflow.

Troubleshooting and verification
--------------------------------

* **Unresolved placeholders:** check the JSON environment LUT, exported variables
  and absolute settings-file path. ``PATH_NAMELIST`` is present in some LUTs, but
  the example namelist location is controlled by ``{path_app}/{file_namelist}``.
* **Executable or shared library unavailable:** check ``library.location``, the
  working-copy directory and the compatible library directories used by that binary.
* **Namelist validation failure:** select the correct model/version and provide
  every mandatory field. Review warnings for ignored/unknown parameter names.
* **Missing restart or forcing:** verify generated times, flags and dated model
  filenames, rather than changing only the runner's reference timestamp.
* **Dry run unexpectedly executes:** HMC's JSON override takes precedence over
  the caller argument. Set it to true before a check run.
* **No S3M simulation starts:** a direct invocation of the shipped script uses
  dry-run mode. Use the explicit ``main(dry_run=False)`` command after validation.

The documentation recipes have been exercised with a small test executable for
both dry-run and execution paths. Namelist, orchestration and execution tests
also cover the reusable toolkit. Those checks do not replace a scientific run
with the real HMC/S3M binaries, domain grids, forcings and restart datasets.
