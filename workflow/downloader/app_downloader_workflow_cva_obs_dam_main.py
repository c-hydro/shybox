#!/usr/bin/python3
"""Download CVA dam observations using the shared observation workflow.

Use app_downloader_workflow_cva_obs_dam_server.json for direct PostgreSQL
access or app_downloader_workflow_cva_obs_dam_local.json with the local SSH tunnel.
"""

from workflow.downloader.app_downloader_workflow_vda_obs_ws_main import main


if __name__ == '__main__':
    main(view_table=True)
