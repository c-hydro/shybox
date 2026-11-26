# Observation downloader

Run hourly station downloads from PostgreSQL using
`app_downloader_workflow_obs_main.py` or its Bash launcher,
`exec_downloader_workflow_obs.sh`. Edit the launcher deployment paths before
using it in a different checkout.

- `app_downloader_workflow_obs_rain_local.json` uses an SSH tunnel.
- `app_downloader_workflow_obs_rain_server.json` uses a direct PostgreSQL connection.

Both profiles enable rain, temperature, humidity, wind, snow and radiation.
Discharge and the three soil saturation depths are configured for optional use.

See the [hourly observation downloader guide](../../docs/downloader_workflows.rst)
for connection setup, variable options, realtime and history commands, and run
reports.
