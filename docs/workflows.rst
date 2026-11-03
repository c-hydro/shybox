Workflow guide and status
=========================

Dataset workflows
-----------------

``workflow/dataset/convert/`` contains drivers for numerical-weather and
observed-data conversion, including IFS, ICON, station-to-grid and
grid-to-time-series applications.

``workflow/dataset/merge/`` combines data by domain or time. Select the HMC or
S3M variant that matches your file formats and model configuration.

``workflow/dataset/disaggregate/`` contains the RFarm ICON workflow. Its purpose
is to transform rainfall forecasts into stochastic ensemble members, with
intermediate results on disk and final model forcing written separately.

Model execution
---------------

``workflow/runner/`` contains HMC and S3M runners. These use utilities under
``shybox/runner_toolkit/`` to prepare namelists and launch model executables.
Supply an executable compatible with the selected namelist template, input
datasets and model version.

Geographic preparation and analysis
-----------------------------------

``workflow/geo/`` provides geographic preparation drivers, including catchment
processing. ``workflow/analyzer/`` contains model-output analysis drivers,
including time-series analysis.

Deployment
----------

Docker images and entrypoints are organized by task under ``docker/``.
Inspect each Dockerfile's repository and branch arguments: builds can fetch
a remote revision rather than using your local working-tree changes.
Apptainer conversion notes and HPC execution material are maintained separately.

Development limitations
-----------------------

This documentation edition is an initial guide, not a certification of every
example workflow. In the inspected development baseline:

* The RFarm adapter is a scaffold. Work on its model call, ancillary file
  collection and sequential output conversion is ongoing.
* The IFS converter imports ``exec_interpolate_data``, while the inspected grid
  adapter defines ``exec_interpolate_data_generic``. Check this interface before
  using that converter as a runnable example.
* Some example paths are specific to development machines or geographic domains.
* The test directory includes manual examples and fixtures; coverage of complete
  scientific workflows is limited.

For RFarm, storing one pickle per ensemble member avoids retaining the entire
ensemble in RAM. Reading a pickle still loads one full member, and the numerical
algorithm retains its own working arrays. Sequential file consumption and
ensemble-specific output paths are required for the final writing stage.
