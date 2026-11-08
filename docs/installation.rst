Installation
============

Scientific environment
----------------------

Use Linux and the supplied Conda setup for the scientific and GIS libraries.
The environment files target **Python 3.12**. Although package metadata declares
Python 3.9 or later, use Python 3.12 for this development setup: some modules
use the standard-library ``tomllib`` module directly.

.. code-block:: bash

   git clone https://github.com/c-hydro/shybox.git
   cd shybox
   git switch main
   bash setup_shybox_base.sh
   source ./conda/shybox_base_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

The ``main`` branch supplies the published documentation and maintained code.
Development-preview chapters describe additional unreleased local work. Pin a
known branch or commit for reproducible runs. The setup script
creates or updates the ``shybox_base_libraries`` environment and writes an
activation helper inside the repository-local ``conda/`` directory.

Check the selected interpreter:

.. code-block:: bash

   python --version
   python -c "import shybox; print(shybox.__version__)"

The base YAML includes NumPy, SciPy, pandas, xarray, NetCDF, GDAL, rasterio,
cfgrib and spatial resampling dependencies. The short dependency list in
``pyproject.toml`` does not describe the complete workflow environment.

Geographic processing
---------------------

For the geographic environment, including ``pysheds``:

.. code-block:: bash

   bash setup_shybox_geo.sh
   source ./conda/shybox_geo_settings
   export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

Source activation helpers from the repository root because they contain
relative paths. For a custom installation prefix, inspect the arguments in
the setup scripts before running them.

Optional external libraries
---------------------------

RFarm and PRISM are configured under ``tool.shybox.external_toolkits`` in
``pyproject.toml``. Their source trees and additional dependencies are maintained
separately. Locating a library is distinct from validating that all of its
runtime dependencies are installed.

RFarm imports Cartopy while loading its model helpers. If your chosen branch's
environment definition does not include it, install it in the environment
that runs the workflow:

.. code-block:: bash

   conda install -n shybox_base_libraries -c conda-forge cartopy

Documentation environment
-------------------------

Documentation has its own small dependency file. It does not require the
scientific environment or model executables. See :doc:`development` for the
build command and :doc:`publishing` for Read the Docs setup.
