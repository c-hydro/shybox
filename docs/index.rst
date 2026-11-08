SHYBOX documentation
====================

.. container:: shybox-hero

   .. image:: _static/images/CVA-170.jpg
      :alt: Alpine lake beneath mountain ridges with patches of snow.

   .. container:: shybox-hero-copy

      **Snow and hydrological modeling**

      From environmental datasets to reproducible workflows.

**SHYBOX** (Snow and HYdrologic toolBOX) provides configurable workflows for
environmental data processing, snow modeling and hydrological modeling.
It is developed by the CIMA Research Foundation for research and operational
forecasting, including Flood-PROOFS systems.

This is documentation edition **0.2.0**, describing the repository structure
and development interfaces of SHYBOX |package_version|. Workflow compatibility
depends on the selected code branch, model version and dataset configuration.

The HMC/S3M runner guide documents the current runner interfaces. The
:doc:`monitoring` and :doc:`hydrapp` chapters are development previews of
unreleased local integrations; their new package paths are not yet available
in the published ``main`` branch. Documentation publication does not release
those application changes.

Start with :doc:`installation` to prepare the scientific environment, then
follow :doc:`quickstart` to configure a workflow for your own datasets.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   overview
   installation
   quickstart
   configuration
   workflows
   runners
   monitoring
   hydrapp

.. toctree::
   :maxdepth: 2
   :caption: Development and releases

   development
   publishing
   releases
   license

Project resources
-----------------

* `SHYBOX source code <https://github.com/c-hydro/shybox>`_
* `SHYDATA datasets <https://github.com/c-hydro/shydata>`_
* `Report a problem <https://github.com/c-hydro/shybox/issues>`_

The documentation build does not import the scientific package, download
datasets or execute hydrological models.
