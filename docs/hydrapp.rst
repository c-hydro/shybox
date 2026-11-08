Hydrapp
========

.. note::

   Development preview: this chapter describes unreleased integrations from
   the development checkout. The new code, configuration paths and examples
   are not yet included in the published GitHub main branch. Publishing this
   documentation does not publish those application changes.


Hydrapp is SHYBOX's Flask viewer, previously located in ``dashboard/``. Its
current application path is ``hydrapp/``. It reads existing model products and
monitoring records; model execution and aggregation stay in SHYBOX workflows.

Start the example
-----------------

From the repository root:

.. code-block:: bash

   bash setup_shybox_hydrapp.sh
   hydrapp/.venv/bin/python -m hydrapp.app --config hydrapp/config.demo.json

Open ``http://127.0.0.1:5050``. Stop with Ctrl+C. The setup script configures the viewer and monitoring dependencies together.
``HYDRAPP_CONFIG`` selects a configuration when ``--config`` is omitted;
``SHYBOX_DASHBOARD_CONFIG`` remains supported.

The portable archive contains 23 HAT image/metadata pairs: 10 rainfall/soil
moisture maps and 13 Foglia section charts. Original publication hours, source
initializations and figure contents are preserved. These archived results are
from 21 October 2026. Demo monitoring records are synthetic and labelled as such.

Pages
-----

* ``/figures``: all published products, with date, time and collection filters.
* ``/maps``: rainfall aggregation windows and instantaneous soil-moisture images.
* ``/figures/time-series``: section charts filtered by basin and source.
* ``/runs``: interactive NetCDF section status maps.
* ``/time-series``: interactive NetCDF hydrographs and ensemble bands.
* ``/monitoring``: application lifecycle, member/retry history, RAM and Ruler reports.

The root page opens the figure catalogue when publications are enabled. A
configuration without publications retains the original section overview.

Configure an archive
---------------------

The ``publications`` configuration accepts ``enabled`` and ``folder``. Paths
are relative to the JSON file. The default points to the included demo figures;
replace it with your archive folder to browse operational HAT results.

``shybox.publishing_toolkit.LegacyFigureCatalog`` reads paired PNG/JSON products:

.. code-block:: text

   figures/
     maps/<collection>/<YYYYmmddHH>/<figure>.png + .json
     time_series/<basin>/<collection>/<YYYYmmddHH>/<figure>.png + .json

``/api/figures`` exposes normalized product metadata and image/metadata URLs.
Products have stable IDs derived from relative paths. Only files inside the
configured archive are served; broken pairs are reported in the catalogue.
The original source archive is read-only. The demo is a local copy.

Publication time and each source's initialization are independent fields. The
legacy time-series metadata describes the image and does not carry numerical
series; the image viewer preserves the original chart. Interactive charts use
the existing NetCDF reader. Published map images include labels/legends and
are displayed as complete figures. Interactive raster overlays, extraction of
pixel values and production aggregation/rendering remain future publishing work.

Monitoring and RAM demos
------------------------

Run with the scientific SHYBOX environment in a second terminal:

.. code-block:: bash

   python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources

This generates success, failure, skipped, dry-run, stale and missing-run examples,
as well as an incomplete ensemble. The analyzer heartbeat stays live while the
script runs. ``--resources`` writes real measurements of the demo process and a
Ruler report; when it finishes, it writes a RAM history image. Use
``--duration 0 --resources`` to refresh snapshots quickly.

See :doc:`monitoring` for process selection, collector configuration and Ruler
provenance. Operational configurations should use their own run declarations
and output folders; ``config.demo.json`` deliberately points to demo records.
