Runner workflows
================

Use the two folders under ``workflow/runner/`` in order: prepare forcing with
``datasets/``, then configure and launch a compiled model with ``execution/``.
The dataset runner writes model inputs; the execution runner consumes them.
Keep separate settings files for these two stages.

Choose your route
-----------------

.. list-table:: Supported starting points
   :header-rows: 1
   :widths: 22 39 39

   * - Run
     - ``runner/datasets``
     - ``runner/execution``
   * - HMC deterministic
     - HMC dataset script with the deterministic forcing JSON.
     - HMC base script with the JSON matching your binary's model version.
   * - HMC rainfall ensemble
     - HMC dataset script with the probabilistic forcing JSON; one output summary per member.
     - Separate model runs with member-specific paths; review the orchestration limitations in :doc:`runners`.
   * - S3M deterministic
     - S3M dataset script and forcing JSON; processes observations and forecasts.
     - S3M base script with the 5.3.3 example adapted to your binary and domain.

First complete run
------------------

1. Install the scientific environment using :doc:`installation`. Prepare terrain,
   upstream observations/forecasts and a compatible compiled HMC or S3M executable.
2. Follow :doc:`dataset_execution`: copy the forcing JSON, export input and output
   roots, set time coverage and field mappings, then run the matching dataset script.
3. Follow :doc:`dataset_summaries`: inspect each completed member's JSON and require
   ``ready == true``. Check the actual file paths, variable names, units and grids.
4. Follow :doc:`runners`: copy the model execution JSON, set its forcing paths,
   simulation interval, restart settings, static data and executable. Generate and
   inspect the namelist with the documented model-specific dry-run command.
5. Launch the model, check its exit status and diagnostic output, then use
   :doc:`monitoring` and :doc:`results` to inspect the run.

The model runner does not automatically import a dataset summary or rewrite its
namelist from it. The summary's inclusive forcing bounds describe available
input times; they are not model restart times or simulation-length parameters.
Use the handoff example in :ref:`runner-dataset-handoff` to connect the stages.

.. toctree::
   :maxdepth: 2

   data_preparation
   model_execution
