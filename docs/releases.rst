Software and documentation releases
======================================================================

SHYBOX 1.7.1 — Dataset execution and member summaries
----------------------------------------------------------------------

Prepared on 14 November 2026 for ``main/1.7.1``. Add per-member model-input
manifests, atomic summary I/O, explicit member collection merging, nested
execution metadata and observed/forecast grouping. The sequential runner retains
execution traces without serializing data handlers and checks completed output
files, including gzip alternatives. Restore planning-only CLI previews and fix
HMC NetCDF missing-value metadata. See :doc:`dataset_execution` and
:doc:`dataset_summaries` for the released interfaces.

The ``datasets_parallel`` folder is a local experiment and is excluded from this
release. The release does not introduce parallel forcing execution.

0.5.0 — Dataset execution and topic navigation
----------------------------------------------------------------------

Prepared on 14 November 2026 for SHYBOX |package_version|. Organize the sidebar
into expandable main topics, guides and section entries while retaining existing
page URLs. Add execution and summary guides, refresh planner/history examples,
and distinguish the archived composer from the released sequential runner.

SHYBOX 1.7.0 — Consolidated main release
----------------------------------------------------------------------

Released on 12 November 2026 as ``main/1.7.0``. This release combines dataset
planning, reorganized HMC/S3M runners, Hydrapp, monitoring,
RainFARM integration and JSON exports. It includes software citation metadata
and an archival import of the HMC labs under ``labs/hmc/``.

Runner entry points now live under ``workflow/runner/execution/``; dataset
preparation profiles live under ``workflow/runner/datasets/``. Analyzer profiles
use explicit deterministic/probabilistic filenames. The legacy labs have not
been migrated to current APIs. See the repository ``CHANGELOG`` for details.

Earlier development snapshots are numbered 1.6.0–1.6.3 in date order. Those
entries document history and do not imply historical tagged releases.
Documentation editions below keep their independent numbering.


0.4.0 — CIMA theme and SHYBOX 1.7.0
----------------------------------------------------------------------

Prepared on 12 November 2026 for SHYBOX |package_version|. Adopt the CIMA
website palette and official logo, add the JupyterLab setup guide, document
the consolidated release and distinguish the unfinished composer prototype
from the supported dataset planner.

0.3.0 — Dataset preparation guide
----------------------------------------------------------------------

Prepared on 10 November 2026 for SHYBOX |package_version|. Documentation is
published from ``main``; Read the Docs ``latest`` follows its successful build.

* Add a dedicated dataset guide with HMC/S3M setup, CLI planning/composition,
  configuration, source mapping, validity times and forecast issue cycles.
* Document DatasetManager file plans, availability windows, forecast fallback,
  ensemble sources, spatial operations, gap policies and atomic publication.
* Refresh the README, repository layout, navigation and related guide links.

The reorganized runner paths and planner are included in SHYBOX 1.7.0.
The forcing composer remains an unfinished design under ``experiments/``.

0.2.0 — Runner guide and development previews
----------------------------------------------------------------------

Prepared on 8 November 2026 for SHYBOX |package_version|. Documentation is
published from ``main`` and built as the Read the Docs ``latest`` version.

* Add HMC/S3M setup recipes, namelist/time settings, dry-run/launch commands,
  ensemble guidance and troubleshooting.
* Add clearly labelled development previews for monitoring, Ruler and Hydrapp;
  the associated uncommitted application code is not part of this publication.
* Validate shell/JSON examples, exercise both base runners with a small test
  executable, and build Sphinx with warnings treated as errors.

The documentation edition does not change the package version or announce a
model/software release. A new page is publicly available only after its remote
Read the Docs build completes successfully.

0.1.0 — First documentation edition
----------------------------------------------------------------------

Prepared on 3 November 2026 for SHYBOX |package_version|, using the ``fproofs``
development branch as the code baseline.

This first edition includes:

* Project overview and the actual repository layout.
* Conda environment setup and workflow configuration guidance.
* Dataset, model-runner and analysis workflow orientation.
* Development status, including the ongoing RFarm ancillary-data integration.
* A standalone Sphinx build with pinned documentation dependencies.
* Read the Docs configuration, import instructions and versioning guidance.

The edition number identifies the documentation. It does not change the package
version or claim a new HMC, S3M or RFarm software release.

Publication status is determined by the GitHub branch and Read the Docs build
dashboard. Preparing or uploading documentation sources does not itself mean
that a public Read the Docs build has completed.
