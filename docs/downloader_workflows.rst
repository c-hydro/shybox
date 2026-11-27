Hourly observation downloader
=============================

The VDA weather-station and CVA dam workflows in ``workflow/downloader/``
read observations from PostgreSQL and write one CSV for each active variable
and hour. Both workflows include local SSH tunnel and direct server profiles.

Setup and connection
--------------------

Install the :doc:`SHYBOX base environment <installation>` and confirm that
Python can import ``psycopg``. The launchers use Bash, GNU ``date`` and
Linux ``flock``.

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - Settings file in ``workflow/downloader/``
     - Connection
   * - ``app_downloader_workflow_vda_obs_ws_server.json``
     - Direct PostgreSQL at ``192.168.7.11:6432`` from a host with access.
   * - ``app_downloader_workflow_vda_obs_ws_local.json``
     - Local SSH forwarding at ``127.0.0.1:16432``.
   * - ``app_downloader_workflow_cva_obs_dam_server.json``
     - Direct PostgreSQL for CVA.
   * - ``app_downloader_workflow_cva_obs_dam_local.json``
     - Local SSH forwarding for CVA.

For the local profile, open an SSH session through your organization's
bastion and keep it running while downloading:

.. code-block:: bash

   ssh -N -T -o ExitOnForwardFailure=yes \
     -L 127.0.0.1:16432:192.168.7.11:6432 user@bastion.example.org

Replace ``user@bastion.example.org`` with your approved SSH login. The
profiles use ``auth_method: auto``, which checks ``~/.netrc`` and then the
configured environment variables. The example settings use
``SHYBOX_DB_USER`` and ``SHYBOX_DB_PASSWORD``. With the local tunnel the
``netrc_host`` setting keeps the credential lookup under the PostgreSQL host.
Use permissions ``0600`` for the credentials file.

Choose variables and outputs
----------------------------

Each key under ``configuration.application.source.variables`` selects a
variable. Its matching key under ``configuration.application.destination``
defines where to write it. Set ``active`` to ``false`` to skip a variable
and its destination. The workflows validate the active source and output pairs
and require at least one active variable.

VDA weather stations
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 38 19 23 20

   * - Variable key
     - Active in examples
     - Hourly treatment
     - Geosupervisor
   * - ``rain``
     - Yes
     - Sum
     - Yes
   * - ``temperature``
     - Yes
     - Mean
     - Yes
   * - ``humidity``, ``wind``, ``radiation``
     - Yes
     - Mean
     - No
   * - ``snow``
     - Yes
     - Instantaneous
     - Yes
   * - ``discharge``
     - No
     - Mean
     - Yes
   * - ``soilsat_10``, ``soilsat_20``, ``soilsat_40``
     - No
     - Instantaneous
     - No

The examples use ``cf-vda``, ``arpa-vda`` and ``piemonte``. The provider also
recognizes ``france`` and ``switzerland``. Use ``quality_level`` 1 for
validity codes of at least −1, or 2 to also require a code below 4.
Instantaneous readings also accept codes −64 and −65. In ``geolimits``, the
four values are minimum latitude, maximum latitude, minimum longitude and
maximum longitude. Use ``null`` for an open bound.

The VDA output columns are ``station``, ``longitude``, ``latitude``,
``code``, ``value`` and ``time``. The examples use semicolon delimiters,
UTF-8 and ``-9999`` for missing fields. Set ``destination.<variable>.options``
to change CSV formatting.

CVA dams
~~~~~~~~

Both CVA profiles enable ``reservoir_water_level`` (database parameter 100,
``LIVELLI-DIGHE``) and ``turbine_discharge`` (parameter 101, ``TURBINATE``).
The provider queries the ``tables_cva`` schema, including inactive stations for
historical downloads. It averages readings on ten-minute marks with validity
codes of at least −1 and rounds to two decimal places. Set ``geolimits`` to
restrict station coordinates. Missing values are written as ``-9999``.

The CSV includes the station ID, name, coordinates, altitude, alert area, time,
value, unit and variable. Any transformation into plant forcing belongs to the
downstream workflow.

Run the downloader
------------------

The launchers declare their deployment paths in the header and default to
the direct PostgreSQL profiles and FloodProofs interpreter. Adjust the header
paths for your deployment. Set ``SHYBOX_PYTHON`` to use another interpreter,
or set ``SHYBOX_CONDA_BIN`` and optionally ``SHYBOX_CONDA_ENV`` for a named
environment. Select another profile with ``--settings-file`` or
``SHYBOX_OBS_SETTINGS_FILE``.

.. code-block:: bash

   workflow/downloader/exec_downloader_workflow_vda_obs_ws.sh realtime
   workflow/downloader/exec_downloader_workflow_cva_obs_dam.sh realtime
   workflow/downloader/exec_downloader_workflow_cva_obs_dam.sh history \
     "2026-11-25 08:00" "2026-11-25 12:00"

Realtime rounds down to the current Europe/Rome hour. The default
``TIME_PERIOD=4`` is a four-hour lookback, which yields five hourly timestamps
because the ends of the interval are included. The optional argument after
``realtime`` sets a different lookback. In history mode, the end is the run
time and the launcher calculates the period from the supplied bounds.

Set ``DOMAIN_NAME``, ``PATH_LOG`` and ``PATH_DATA`` to route outputs.
To call the Python application directly, activate the base environment first
and set the same variables along with ``TIME_PERIOD``:

.. code-block:: bash

   export DOMAIN_NAME=vda PATH_LOG="$PWD/run/log" PATH_DATA="$PWD/run/data"
   export TIME_PERIOD=4
   python -m workflow.downloader.app_downloader_workflow_cva_obs_dam_main \
     -settings_file "$PWD/workflow/downloader/app_downloader_workflow_cva_obs_dam_local.json" \
     -time "2026-11-25 12:00"

Run reports and locks
---------------------

The launchers use ``lock_<run_name>_<run_variable>_<YYYYMMDD_HH>.lock`` and
write ``report_<run_name>_<run_variable>_<YYYYMMDD_HH>.env`` after the run
finishes. The mode is ``realtime`` or ``history``. The default run label is
``rain`` for VDA and ``dam`` for CVA. ``SHYBOX_RUN_VARIABLE`` changes the
label. The JSON settings control which observations are downloaded.

An invocation with the same lock key exits with code 75 while the first
run holds the lock. The lock is released at process exit. Set
``SHYBOX_LOCK_DIR`` and ``SHYBOX_REPORT_DIR`` to change the file locations.

Use ``-f`` or ``--force`` to run despite a held lock. The forced run writes
``report_<run_name>_<run_variable>_<YYYYMMDD_HH>_forced_<PID>.env`` and
preserves the lock and report of the active run. When the lock is available,
the launcher acquires it and uses the standard report file.

A Bash consumer can source the environment report and check ``STATUS``
(``success`` or ``failed``), ``TIME_RUN``, ``TIME_START``, ``TIME_END``,
``TIME_PERIOD``, ``EXECUTION_TIME`` in seconds, and ``EXIT_CODE``.

Provider interface
------------------

``configuration.application.source.provider`` selects the access point.
The examples use the package ``shybox.db_toolkit.access_point_db`` and the files
``access_point_vda.py`` and ``access_point_cva.py``. The loader also accepts a
dotted module name or the ``vda`` alias. A provider supplies
``get_data(source, selection, start, end)``
and can override ``write_data()``. Providers that use the default CSV writer
can pass their own column mapping or use their input columns.
