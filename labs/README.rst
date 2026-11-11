Training labs
=============

``hmc/`` contains an archival import of the Flood-PROOFS HMC training labs.
Read ``hmc/IMPORT.rst`` for provenance and scope and ``hmc/README.rst`` for
the original usage instructions. These labs are separate from the installed
SHYBOX package and current workflow examples.

JupyterLab environment
----------------------

Keep the notebooks in this repository and use a separate environment. From the
repository root::

   ./setup_shybox_jupyter.sh --python python3.11
   ./run_shybox_jupyter.sh

The setup creates ``labs/.venv`` and installs ``labs/requirements.txt``. It does
not change the SHYBOX scientific environment. Python 3.10–3.11 is required for
legacy helpers, which still import ``pipes``. Dependency ranges preserve older
pandas, xarray and plotting interfaces; complete notebook execution still needs
local datasets, configuration and a separate HMC executable.

The launcher opens ``labs/hmc`` as the working directory and Jupyter root, sets
helper import paths and passes additional options to JupyterLab::

   ./run_shybox_jupyter.sh -- --no-browser --port=8889

Both scripts accept ``--venv /absolute/path/to/environment``. To make this
kernel available in an existing JupyterLab installation, run setup with
``--register-user-kernel`` and select **SHYBOX HMC Labs** in that installation.
Open the repository notebooks directly; no copying into JupyterLab is needed.
The original environment manager in ``hmc/`` is retained for provenance; use
the new scripts for this checkout.
