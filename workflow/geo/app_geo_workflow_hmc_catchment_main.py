#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW GEO WATERSHEDS - HMC

__date__ = '20260819'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME=marche;
PATH_DST_BASE=/home/fabio/Desktop/shybox/dset/case_study_marche/geo_hmc/dst/;
PATH_LOG=/home/fabio/Desktop/shybox/exec/case_study_marche/geo_hmc/log/;
PATH_SRC_BASE=$HOME/Desktop/shybox/dset/case_study_marche/geo_hmc/src/;
PATH_TMP=$HOME//Desktop/shybox/exec/case_study_marche/geo_hmc/tmp;
PATH_APP=$HOME//Desktop/shybox/

Version(s):
20260819 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_geo_toolkit.lib_proc_compute_catchment import delineate_watershed

from shybox.orchestrator_toolkit.orchestrator_handler_geo import OrchestratorGeo as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for geo watersheds configuration'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-08-19'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(
        handlers=['stream'],
        settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, _ = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        src=alg_args_file,
        root_key="configuration",
        application_key=None
    )

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application')
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False,
        time_keys=(), template_keys=()
    )
    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=True)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow')
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get application logging
    alg_app_log = alg_cfg_obj.get_application("log", root_key=None)
    # fill application logging
    alg_app_log = alg_app_log.resolved(
        time_values=None,  # no fill_section_with_times
        when=None,  # no LUT time resolution
        strict=False,
        resolve_time_placeholders=False,  # do NOT turn time_* into strftime strings
        expand_env=True,  # BUT expand $HOME, $RUN, ...
        env_extra=None,  # or {"RUN": "base"} etc
        validate_result=False,  # or True + allow_placeholders=True if needed
        validate_allow_placeholders=True,
        validate_allow_none=False,
    )
    # view application logging
    alg_cfg_obj.view(section=alg_app_log, table_name='application [cfg application logging]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_app_log['path'], logger_file=alg_app_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=alg_app_log.get('handlers', ['stream']),
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_geo_hmc_watersheds",
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO START
    # info algorithm (start)
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging_handle.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO DATASETS SOURCE
    # geo registry sections
    geo_data_sections = DataLocal(
        path=alg_cfg_application['geo_data_src']['registry_sections']['path'],
        file_name=alg_cfg_application['geo_data_src']['registry_sections']['file_name'],
        data_layout='points',
        file_type='points_section_db', file_format='ascii', file_mode='local',
        file_args={
            'lut_map': alg_cfg_application['geo_data_src']['registry_sections']['lut_map'],
            'lut_type': alg_cfg_application['geo_data_src']['registry_sections']['lut_type'],
            'sep':',', 'col_datafrom': 'SEC_TAG'},
        file_variable='sections', file_io='input',
        variable_template={
            "dims_point": {"x": "fields", "y": "sections"},
            "vars_data": {"sections": "sections"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )

    # geo flow directions
    geo_grid = DataLocal(
        path=alg_cfg_application['geo_data_src']['flow_directions']['path'],
        file_name=alg_cfg_application['geo_data_src']['flow_directions']['file_name'],
        file_type='grid_obj', file_format='ascii', file_mode='local', file_variable='fdir', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )

    # geo flow directions
    geo_data_fdir = DataLocal(
        path=alg_cfg_application['geo_data_src']['flow_directions']['path'],
        file_name=alg_cfg_application['geo_data_src']['flow_directions']['file_name'],
        file_type='grid_geo', file_format='ascii', file_mode='local', file_variable='fdir', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )

    # geo channels network
    geo_data_cnet = DataLocal(
        path=alg_cfg_application['geo_data_src']['channels_network']['path'],
        file_name=alg_cfg_application['geo_data_src']['channels_network']['file_name'],
        file_type='grid_geo', file_format='ascii', file_mode='local', file_variable='cnet', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO DATASETS AUXILIARY
    # geo data src (and deps)
    geo_data_src = DataLocal(
        path=None,
        file_name=None,
        file_type=None, file_format='tmp', file_mode='local',
        file_variable='WATERSHEDS' , file_io='derived',
        file_deps=[geo_data_fdir, geo_data_cnet, geo_data_sections],
        variable_template={
            "dims_data": {"x": "longitude", "y": "latitude"},
            "coord_data": {"x": "longitude", "y": "latitude"},
            "vars_data": {
                "fdir": "flow_directions",
                "cnet": "channels_network",
                "sections": "sections",
            }
        },
        logger=logging_handle
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO DATASETS DESTINATION
    # destination data discharge handler
    geo_data_dst = DataLocal(
        path=alg_cfg_application['geo_data_dst']['path'],
        file_name=alg_cfg_application['geo_data_dst']['file_name'],
        data_layout='geo', file_args={},
        file_type='geo_watershed', file_format='tiff', file_mode='local',
        file_variable='WATERSHEDS', file_io='output',
        variable_template={
            "dims_data": {"x": "longitude", "y": "latitude"},
            "coord_data": {"x": "longitude", "y": "latitude"},
            "vars_data": {
                "mask": "watershed_mask"
            },
            "units": {
                "mask": "-"
            }
        },
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ORCHESTRATOR MANAGEMENT
    # orchestrator settings
    orc_process = Orchestrator.watersheds(
        data_package_in=geo_data_src,
        data_package_out=geo_data_dst,
        data_ref={'grid': geo_grid},
        priority=None,
        configuration=alg_cfg_workflow,
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')',
                               blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
