Current release
======================================================================

SHYBOX 1.7.5 — VDA station CSV metadata and quality codes
----------------------------------------------------------------------

Prepared on 27 November 2026. The VDA weather-station downloader writes the
full observation metadata after the original six CSV columns. Finite
PostgreSQL quality codes are preserved. Missing or non-finite codes default
to zero, as in the MATLAB Drops workflow. Missing measurements keep the
configured marker. See :doc:`downloader_workflows` for the CSV schema and
quality-code handling.

SHYBOX 1.7.4 — Hourly database observations
----------------------------------------------------------------------

Prepared on 26 November 2026. The VDA and CVA workflows read hourly data
from PostgreSQL and write one CSV per active variable and hour. The CVA
provider covers reservoir water level and turbine discharge. Both workflows
ship with local SSH tunnel and direct server profiles. The realtime and
history launchers use per-hour locks and write environment reports. See
:doc:`downloader_workflows` for settings, commands and report fields.

SHYBOX 1.7.3 — Deterministic and probabilistic time-series analysis
----------------------------------------------------------------------

Prepared on 20 November 2026. The HMC/Continuum analyzer provides separate
deterministic and probabilistic entry points, each with NetCDF and hydrograph
JSON examples. Ensemble outputs retain configured member IDs and missing
series, with shared observations on the common time and section axes. NetCDF
includes per-section column offsets, and JSON uses member-suffixed field names
for simulated series. See :ref:`hmc-time-series-analysis` for commands and
output layouts.

The :download:`changelog <../CHANGELOG>` contains the release history.
