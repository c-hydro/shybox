Configuration and processing
============================

Workflow settings combine dataset descriptions, processing functions and
runtime values. The JSON schema varies between generations of drivers. Always
start from the example paired with your selected Python driver.

Configuration sections
----------------------

Recent examples use this organization:

.. code-block:: json

   {
     "configuration": {
       "priority": {"reference": "environment", "other": "user"},
       "flags": {},
       "variables": {
         "lut": {"user": {}, "environment": {}},
         "format": {},
         "template": {}
       },
       "application": {}
     },
     "workflow": {
       "options": {},
       "process_list": {}
     }
   }

This fragment illustrates structure only; a complete configuration needs the
application sections and variable definitions required by its driver. Older
examples may use a ``settings`` root instead of ``configuration``.

Placeholders
------------

``variables.lut`` maps logical names to user values or environment variables.
For example, ``"path_root_source": "PATH_SRC"`` lets an environment value
supply a source path. ``format`` describes value types; ``template`` describes
time formatting and other substitutions.

Application sections use placeholders such as ``{domain_name}``,
``{file_time_source}`` and ``{path_time_destination}``. Configuration handling
resolves environment and time values in stages. An ensemble placeholder may
remain unresolved until an individual member is written.

Processing functions
--------------------

The orchestrator locates functions registered with ``@as_process``. Importing
a decorated function in the driver performs that registration. A JSON process
entry selects the registered function and its arguments:

.. code-block:: json

   {
     "function": "convert_rain_units",
     "datasets": {"data": "rain"},
     "results": {"data": "rain"},
     "args": {"to_mm": true}
   }

Dataset mappings must agree with the names supplied by the driver. Changes to
function names, return types or coordinate conventions need matching updates
to the driver and its JSON.

Temporary and ancillary data
----------------------------

General workflow scratch options include ``intermediate_output: "Tmp"`` and
``tmp_dir``. These options do not automatically make every numerical operation
stream from disk.

The RFarm integration under development distinguishes ensemble pickle files
as **ancillary data**. Its proposed application section is:

.. code-block:: json

   "data_ancillary": {
     "path": "{path_tmp}/{domain_name}/{path_time_destination}/{path_time_run}/",
     "file_name": "rf.icon-2i.{file_time_source}_{ensemble}.pkl"
   }

``data_ancillary`` is a sibling of ``data_source`` and ``data_destination``.
The workflow main resolves its settings into a temporary-storage object and
passes it to the RFarm adapter. This interface is ongoing work and is not part
of the committed development baseline used by this first documentation edition.
