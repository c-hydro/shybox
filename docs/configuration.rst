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
     "file_name": "rf.icon-2i.{file_time_source}_{ensemble}.pkl",
     "writer_type": "pickle",
     "prefix": "rfarm_"
   }

``data_ancillary`` is a sibling of ``data_source`` and ``data_destination``.
The workflow main resolves its settings into a temporary-storage object and
passes it to the RFarm adapter. This interface is ongoing work and is not part
of the committed development baseline used by this first documentation edition.

Generic temporary storage
-------------------------

The temporary-data handler under development accepts any filename or named
template. ``folder_name`` selects its root, ``writer_type`` selects ``pickle``,
``netcdf`` or ``zarr``, and ``prefix`` controls isolated run-directory names
(default ``tmp_``). If omitted, ``file_name`` defaults to ``data.pkl``,
``data.nc`` or ``data.zarr``, according to the writer.

.. code-block:: python

   from shybox.dataset_toolkit.dataset_handler_temporary import DataTemporary

   storage = DataTemporary(
       folder_name="/tmp/shybox",
       file_name="field_{step:03d}.nc",
       writer_type="netcdf",
       prefix="processing_",
   ).create_run()

   storage.write_data(data_array, tags={"step": 1})
   with storage.read_data(tags={"step": 1}) as data:
       values = data.isel(time=0).values

Pickle supports arbitrary Python objects. NetCDF and Zarr support xarray
DataArrays and Datasets, preserving their type, names, coordinates and
attributes. Their reads stay lazy unless ``load=True`` is passed; close lazy
objects when finished. Additional writer/reader keyword arguments are forwarded
to the respective backend. Zarr requires the ``zarr`` package and defaults to
format 2; writers can select ``zarr_format=3`` explicitly.

RFarm's native writer still produces pickles. When another ancillary format is
selected, the adapter converts one member at a time and returns paths to the
selected output format. Native pickles remain in the run's ``native`` directory
for caller-managed cleanup. Change the configured filename extension alongside
``writer_type`` (for example, ``.nc`` for ``netcdf`` or ``.zarr`` for ``zarr``).
