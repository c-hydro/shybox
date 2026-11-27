# Observation downloaders

The VDA weather-station and CVA dam workflows query PostgreSQL and write
hourly CSVs.

| Workflow | Launcher | Python module |
| --- | --- | --- |
| VDA weather stations | `exec_downloader_workflow_vda_obs_ws.sh` | `app_downloader_workflow_vda_obs_ws_main.py` |
| CVA dams | `exec_downloader_workflow_cva_obs_dam.sh` | `app_downloader_workflow_cva_obs_dam_main.py` |

Each workflow has a `_local.json` profile for an SSH tunnel and a
`_server.json` profile for direct PostgreSQL access. The CVA profiles enable
reservoir water level and turbine discharge. The VDA profiles enable rain,
temperature, humidity, wind, snow and radiation. Discharge and soil
saturation are available as optional VDA variables.

See the [hourly observation downloader guide](../../docs/downloader_workflows.rst)
for connection settings, deployment paths, realtime and history commands, and
lock and report formats.
