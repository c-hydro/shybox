Configure a first workflow
==========================

Before executing a workflow, obtain the required input datasets, geographic
reference files and, for model runs, a compatible executable and restart data.
Examples in this repository refer to particular domains and local directories;
adapt them to your data layout.

Choose a driver
---------------

Select a Python driver and its matching JSON example from ``workflow/``.
For example, the IFS conversion pair is:

.. code-block:: text

   workflow/dataset/convert/app_converter_workflow_hmc_destine_ifs.py
   workflow/dataset/convert/app_converter_workflow_hmc_destine_ifs.json

Copy the settings to an experiment directory so that you can track changes
without editing the reference example:

.. code-block:: bash

   mkdir -p exec/my_experiment
   cp workflow/dataset/convert/app_converter_workflow_hmc_destine_ifs.json \
      exec/my_experiment/converter_ifs.json

Set paths and times
-------------------

Inspect the JSON environment lookup table. For this example, define values
such as the following, replacing all paths and dates with your own inputs:

.. code-block:: bash

   export DOMAIN_NAME="my_domain"
   export PATH_GEO="/path/to/geographic/reference"
   export PATH_SRC="/path/to/input/ifs"
   export PATH_DST="/path/to/output"
   export PATH_LOG="/path/to/logs"
   export PATH_TMP="/path/to/scratch"
   export TIME_START="2025-11-17 00:00"
   export TIME_END="2025-11-17 00:00"
   export TIME_PERIOD="24"

Check the terrain filename, GRIB variable names, units, source timestamps and
output templates in the JSON. Different drivers may require different settings.

Run the workflow
----------------

With the scientific environment active and the repository on ``PYTHONPATH``:

.. code-block:: bash

   python workflow/dataset/convert/app_converter_workflow_hmc_destine_ifs.py \
      -settings_file "$PWD/exec/my_experiment/converter_ifs.json"

This is a command template, not a bundled runnable demonstration. Some drivers
also accept ``-time``; verify that the selected driver's implementation uses it
before relying on it to override JSON time settings.

Review the log and verify the output grid, timestamps, units and missing-value
handling before using converted data as model forcing. Development branches
can contain incomplete interfaces; see :doc:`workflows` for current limitations.
