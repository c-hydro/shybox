HMC training labs and JupyterLab
======================================================================

The legacy Flood-PROOFS HMC training material is kept in ``labs/hmc/`` with
its original notebooks, scripts, helper library, artwork, authors and licences.
It is an archival import, not a migration to current SHYBOX APIs.

Create a separate JupyterLab environment from the repository root::

   ./setup_shybox_jupyter.sh --python python3.11
   ./run_shybox_jupyter.sh

The setup creates ``labs/.venv`` using Python 3.10–3.11 and installs
``labs/requirements.txt``. The launcher opens ``labs/hmc`` with the helper
libraries on the import path. Both scripts accept ``--venv PATH``; no activation
is needed. To use an existing JupyterLab installation instead, add
``--register-user-kernel`` to setup and select **SHYBOX HMC Labs** there.

Forward Jupyter options after ``--``::

   ./run_shybox_jupyter.sh -- --no-browser --port=8889

Read ``labs/hmc/IMPORT.rst`` and the original ``README.rst`` before execution.
Update domain paths, dates and external services in the JSON profiles and
supply the required datasets and model executable. Opening a notebook does
not execute it or download its scientific inputs.
