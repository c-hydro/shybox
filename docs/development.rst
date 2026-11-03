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
