Documentation releases
======================

0.3.0 — Dataset preparation guide
---------------------------------

Prepared on 10 November 2026 for SHYBOX |package_version|. Documentation is
published from ``main``; Read the Docs ``latest`` follows its successful build.

* Add a dedicated dataset guide with HMC/S3M setup, CLI planning/composition,
  configuration, source mapping, validity times and forecast issue cycles.
* Document DatasetManager file plans, availability windows, forecast fallback,
  ensemble sources, spatial operations, gap policies and atomic publication.
* Refresh the README, repository layout, navigation and related guide links.

The dataset composer and reorganized runner paths describe the development
checkout. Publishing these pages alone does not publish application code.

0.2.0 — Runner guide and development previews
---------------------------------------------

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
-----------------------------------

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
