Overview and repository structure
=================================

SHYBOX separates reusable processing code, workflow configuration and data
distribution. It supports workflows around HMC (Hydrological Modeling Chain)
and S3M (Distributed Snow Simulation System), alongside dataset conversion,
interpolation, merging and analysis.

Repository layout
-----------------

.. code-block:: text

   shybox/
   ├── shybox/                    Reusable Python package
   ├── workflow/                  Workflow drivers and JSON examples
   │   ├── dataset/               Convert, merge and disaggregate data
   │   ├── geo/                   Prepare geographic information
   │   ├── runner/                Configure and execute HMC/S3M
   │   └── analyzer/              Analyze model output
   ├── apps/                      Additional applications
   ├── web/                       Flask result visualization interface
   ├── exec/                      Case-study execution directories
   ├── docker/                    Container definitions and entrypoints
   ├── apptainer/                 Apptainer deployment notes
   ├── hpc/                       HPC execution material
   ├── utils/                     Standalone utilities
   ├── tests/                     Checks, examples and fixtures
   ├── docs/                      Documentation
   ├── pyproject.toml             Package metadata
   └── setup_shybox_*.sh          Conda environment setup

Some local checkouts also contain ``conda/``, ``dset/`` and ``tmp/``. These
hold environments, datasets and temporary files and are ignored by Git.

Core package
------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Toolkit
     - Responsibility
   * - ``config_toolkit``
     - Read settings, command-line arguments and placeholders.
   * - ``dataset_toolkit`` / ``io_toolkit``
     - Describe datasets and read/write their storage formats.
   * - ``orchestrator_toolkit``
     - Connect registered processing functions into workflows.
   * - ``processing_*_toolkit``
     - Implement grid, point, time-series, geographic and hydro calculations.
   * - ``runner_toolkit``
     - Prepare model namelists and manage execution.
   * - ``external_toolkit``
     - Locate separately maintained libraries such as RFarm and PRISM.
   * - ``logging_toolkit`` / ``time_toolkit``
     - Provide shared logging and time handling.

Data and models
---------------

Obtain datasets separately from `SHYDATA <https://github.com/c-hydro/shydata>`_
or your operational data sources. Dataset paths, geographic domains, model
executables and model versions must match the workflow settings. A Python
environment alone does not provide the compiled HMC/S3M engines or their inputs.
