HMC labs import
===============

Imported on 12 November 2026 from the supplied ``fp-labs-hmc-master`` folder.
The original directory layout is preserved so notebook, configuration, library
and image references remain together. Git metadata, Python bytecode and notebook
checkpoint directories are excluded.

* ``fp_labs_analyzer_hmc_*.ipynb``: original map and time-series notebooks.
* ``fp_labs_analyzer_hmc_*.py`` and JSON: analyzer scripts and settings.
* ``fp_labs_downloader_*``: downloader scripts, launchers and dated examples.
* ``fp_labs_manager_hmc_notebook_envs.sh``: legacy environment setup.
* ``library/``, ``img/`` and ``docs/``: bundled helpers and supporting material.
* ``README.rst``, ``AUTHORS.rst``, ``CHANGELOG.rst`` and ``LICENSE.rst``:
  original documentation, credits, history and MIT licence.

This import only organizes the existing material. It has not been migrated or
validated against SHYBOX 1.7.0. Review external services, absolute paths, dates,
dependencies and environment commands before execution. Model binaries and
domain datasets are supplied separately. The original notebooks retain their
saved outputs. Bundled components retain their own licence notices, including
``library/jupyter/LICENSE.rst``.
