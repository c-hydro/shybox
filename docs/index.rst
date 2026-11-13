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

This is documentation edition |docs_version|, describing the repository structure
and development interfaces of SHYBOX |package_version|. Workflow compatibility
depends on the selected code branch, model version and dataset configuration.

Start with :doc:`getting_started`, then follow :doc:`data_preparation` to
plan forcing files, execute the sequential dataset runner and export member
summaries for the model. Continue with :doc:`model_execution` to launch HMC/S3M
and :doc:`results` to inspect and monitor their outputs.

The sidebar groups guides by main topic. Expand a topic to see its guides,
then expand a guide to see its sections. Existing guide URLs remain available.
The parallel dataset runner is an unpublished experiment and is outside this
release. The unfinished composer is documented separately as an archived design.

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Main topics

   getting_started
   data_preparation
   model_execution
   results
   training
   contributor_guide

Project resources
-----------------

* `SHYBOX source code <https://github.com/c-hydro/shybox>`_
* `SHYDATA datasets <https://github.com/c-hydro/shydata>`_
* `Report a problem <https://github.com/c-hydro/shybox/issues>`_

The documentation build does not import the scientific package, download
datasets or execute hydrological models.
