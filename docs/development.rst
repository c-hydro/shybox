Development
===========

Place new code
--------------

Add reusable calculations to the appropriate ``processing_*_toolkit`` inside
``shybox/``. Keep file-format handling in ``io_toolkit`` and dataset access in
``dataset_toolkit``. A workflow driver should coordinate these components and
obtain site-specific paths and parameters from its JSON settings.

For a new configurable processing step:

#. Define its xarray/pandas input and output conventions, units and missing values.
#. Register it with ``@as_process`` and import it in the workflow driver.
#. Add the function name and argument mappings to the JSON ``process_list``.
#. Check a small input/output example, including coordinates and timestamps.

Build the documentation
-----------------------

Use Python 3.12 or later in a separate virtual environment:

.. code-block:: bash

   python3.12 -m venv /tmp/shybox-docs-venv
   /tmp/shybox-docs-venv/bin/python -m pip install -r docs/requirements.txt
   /tmp/shybox-docs-venv/bin/python -m sphinx \
      -b html -W --keep-going -n docs docs/_build/html

Open ``docs/_build/html/index.html`` to review the generated site. The build
fails on warnings, including broken internal references and missing documents.
Documentation source uses reStructuredText and the Read the Docs Sphinx theme.

Keep documentation images in ``docs/_static/images/``. Model illustrations are
organized under ``models/hmc/`` and ``models/s3m/`` within that directory; their
source, attribution and licence notes are recorded in ``models/SOURCES.txt``.
Use relative ``figure`` / ``image`` paths with alternative text and a source
credit. Copy referenced assets into the documentation so builds do not depend
on a developer's filesystem or an external image host.

``docs/conf.py`` reads package metadata from ``pyproject.toml`` without importing
``shybox``. Documentation builds therefore need neither Conda's GIS stack nor
external toolkit downloads.

Public documentation sources
----------------------------

Only the pages in the Sphinx navigation are intended for publication. Existing
IFS scratch material, meteorological data and local operational notes are not
included in the published source set. Keep account details, local access notes
and large datasets outside documentation releases.

GitHub Actions builds the same documentation on documentation changes and
pull requests. Read the Docs uses ``.readthedocs.yaml`` from the selected Git
revision. Neither process runs the scientific models.

Automated checks
----------------

The monitoring, resource, Hydrapp and expanded model/geo test suites below are
included in SHYBOX 1.7.0.

Run from the repository root using the SHYBOX scientific environment with Flask
and psutil installed. The Hydrapp-only environment covers viewer dependencies;
the model/geo suites also need the scientific/GIS environment. Hydrapp requirements
include psutil and Matplotlib for resource collection and plotting.

.. code-block:: bash

   python -B -m unittest discover -s tests/dataset -p 'test_*.py' -v
   python -B -m unittest discover -s tests/runner -v
   python -B -m unittest discover -s tests/geo -v
   python -B -m unittest discover -s tests/monitoring -v
   python -B -m unittest discover -s hydrapp/tests -v
   python -B -m unittest discover -s tests/io -v
   python -B -m unittest discover -s tests/orchestrator -v

Dataset coverage includes validity-time/cycle selection, deterministic/ensemble
modes, file plans, observation-to-forecast fallback, source formats, interpolation,
prepared-file copies, gap policies and model forcing round trips. See
:doc:`datasets` for configuration and planning commands.

Runner coverage includes all registered HMC/S3M namelist versions, mandatory
fields, Fortran round trips, output files, deterministic/parallel ensemble
scheduling, retries, failures and member isolation. The HMC execution bridge
rejects nonzero or unknown exit codes. Geo tests use small synthetic grids to
verify orientation, coordinate drift/bounds and metric/geographic catchment area.
Resource tests sample real small processes and check CSV/PNG and Ruler output.

These checks do not compile or run native HMC/S3M simulations or validate forecast
skill. Optional RainFARM/pysheds integrations and operational datasets need their
own environments. Existing external-workspace examples are not a hermetic test
suite. Avoid inheriting incompatible GIS/HDF5 library paths when running checks.
