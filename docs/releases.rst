Current release
======================================================================

SHYBOX 1.7.4 — Hourly database observations
----------------------------------------------------------------------

Prepared on 26 November 2026. The observation downloader reads hourly station
data from PostgreSQL and writes a CSV for each active variable and hour. The
VDA profiles support a direct server connection and a local SSH tunnel. The
Bash launcher checks the Conda environment, locks each mode and run hour, and
publishes JSON and environment reports. See :doc:`downloader_workflows` for
settings, commands and report fields.

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
