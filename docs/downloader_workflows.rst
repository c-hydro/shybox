Hourly observation downloader
=============================

The application in ``workflow/downloader/`` reads station observations from
PostgreSQL and writes one CSV for each active variable and hour. It includes
local and server profiles for the Valle d'Aosta station database.

Setup and connection
--------------------

Install the :doc:`SHYBOX base environment <installation>` and confirm that
Python can import ``psycopg``. The Bash launcher also uses ``python3`` for
run reports and Linux ``flock`` for locking.

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - Settings file in ``workflow/downloader/``
     - Connection
   * - ``app_downloader_workflow_obs_rain_server.json``
     - Direct PostgreSQL at ``192.168.7.11:6432`` from a host with access.
   * - ``app_downloader_workflow_obs_rain_local.json``
     - Local SSH forwarding at ``127.0.0.1:16432``.

For the local profile, open an SSH session through your organization's
bastion and keep it running while downloading:

.. code-block:: bash

   ssh -N -T -o ExitOnForwardFailure=yes \
     -L 127.0.0.1:16432:192.168.7.11:6432 user@bastion.example.org

Replace ``user@bastion.example.org`` with your approved SSH login. Both
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
validity codes of at least −1, or 2 to apply an upper bound of 4 as well.
Instantaneous readings also accept codes −64 and −65. In ``geolimits``, the
four values are minimum latitude, maximum latitude, minimum longitude and
maximum longitude. Use ``null`` for an open bound.

The VDA output columns are ``station``, ``longitude``, ``latitude``,
``code``, ``value`` and ``time``. The examples use semicolon delimiters,
UTF-8 and ``-9999`` for missing fields. Set ``destination.<variable>.options``
to change CSV formatting.

Run the downloader
------------------

Set the folder, application and default settings paths at the top of
``workflow/downloader/exec_downloader_workflow_obs.sh`` for your deployment.
The launcher checks the selected environment before running. It defaults to
``shybox_base_libraries``. Use ``SHYBOX_CONDA_ENV``, ``SHYBOX_CONDA_BIN``
or ``SHYBOX_PYTHON`` to select another installed environment or interpreter.

.. code-block:: bash

   workflow/downloader/exec_downloader_workflow_obs.sh realtime
   workflow/downloader/exec_downloader_workflow_obs.sh history \
     "2026-11-25 08:00" "2026-11-25 12:00"
   workflow/downloader/exec_downloader_workflow_obs.sh \
     --settings-file workflow/downloader/app_downloader_workflow_obs_rain_local.json \
     realtime

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
   python workflow/downloader/app_downloader_workflow_obs_main.py \
     -settings_file workflow/downloader/app_downloader_workflow_obs_rain_local.json \
     -time "2026-11-25 12:00"

Run reports and locks
---------------------

The Bash launcher guards each mode and run hour with
``lock_<run_name>_<YYYYMMDD_HH>.lock``. The run name is ``realtime`` or
``history``. An overlapping invocation for that key exits with code 75, leaving
the active run's report accessible. The lock is released at process exit.

The launcher writes ``report_<run_name>_<YYYYMMDD_HH>.env`` and a JSON record.
The JSON directory defaults to ``$PATH_LOG/$DOMAIN_NAME/runs``. The lock and
report folders are declared separately in the Bash script. Override the three
locations with ``SHYBOX_LOCK_DIR``, ``SHYBOX_REPORT_DIR`` and
``SHYBOX_RUN_DIR``.

The reports contain run bounds, elapsed seconds, exit code and a status of
``running``, ``success``, ``failed`` or ``interrupted``. A Bash consumer can
source the ``.env`` report and check ``STATUS``, ``TIME_RUN``,
``EXECUTION_TIME`` and ``EXIT_CODE`` before starting downstream processing.

Provider interface
------------------

``configuration.application.source.provider`` selects the VDA access point.
The examples use the package ``shybox.db_toolkit.access_point_db`` and file
``access_point_vda.py``. The loader also accepts a dotted module name or
the ``vda`` alias. A provider supplies ``get_data(source, selection, start, end)``
and can override ``write_data()``. Providers that use the default CSV writer
can pass their own column mapping or use their input columns.
