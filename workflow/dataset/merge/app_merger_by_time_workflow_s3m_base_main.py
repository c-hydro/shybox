#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW MERGER BY TIME BASE

__date__ = '20260126'
__version__ = '1.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='italy';
TIME_START="'2010-07-01 00:00'";
TIME_END="'2010-07-02 12:00'";
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_itwater/merger_s3m_by_time/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_itwater/merger_s3m_by_time/data/';
PATH_DST='/home/fabio/Desktop/shybox/exec/case_study_itwater/merger_s3m_by_time/data/';
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_itwater/merger_s3m_by_time/log/;
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_itwater/merger_s3m_by_time/tmp/

Version(s):
20260126 (1.1.0) --> Refactor using class methods in shybox package
20250403 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

import pandas as pd

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorGrid as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_toolkit.lib_proc_merge import merge_data_by_time

from shybox.time_toolkit.lib_utils_time import select_time_range, select_time_format
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for datasets merger by time base configuration'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2026-01-26'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(src=alg_args_file, add_other_keys_to_mandatory=True,
                                            root_key=None, application_key=None)

    # view lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut', root_key=None, raise_if_missing=True)
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='variables [cfg info]', table_print=view_table)

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application', root_key=None, raise_if_missing=True)
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False,
        time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=('file_source_time', 'path_destination_time', 'file_destination_time')
    )
    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=view_table)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow', root_key=None, raise_if_missing=True)
    # fill workflow section
    alg_cfg_workflow = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_workflow, lut=alg_cfg_lut,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=()
    )
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow [cfg info]', table_print=view_table)

    # get tmp section
    alg_cfg_tmp = alg_cfg_obj.get_section(section='tmp', root_key=None, raise_if_missing=True)
    # fill tmp section
    alg_cfg_tmp = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_tmp, lut=alg_cfg_lut,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=()
    )

    # view tmp section
    alg_cfg_obj.view(section=alg_cfg_tmp, table_name='tmp [cfg info]', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get logging section
    alg_cfg_log = alg_cfg_obj.get_section("log", root_key=None, raise_if_missing=True)
    # fill logging section
    alg_cfg_log = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_log, lut=alg_cfg_lut,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=()
    )
    # view log section
    alg_cfg_obj.view(section=alg_cfg_log, table_name='log [cfg info]', table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_log['path'], logger_file=alg_cfg_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_merger_by_time_s3m",
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
    ## TIME MANAGEMENT (GLOBAL)
    # Time generic configuration
    alg_time_generic = select_time_range(
        time_start=alg_cfg_application['time']['start'],
        time_end=alg_cfg_application['time']['end'],
        time_frequency=alg_cfg_application['time']['frequency'],
        ensure_range=False, flat_if_single=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## GEO MANAGEMENT
    # geographic reference
    geo_data = DataLocal(
        path=alg_cfg_application['geo']['terrain']['path'],
        file_name=alg_cfg_application['geo']['terrain']['file_name'],
        file_type='grid_2d', file_format='ascii', file_mode='local', file_variable='terrain', file_io='input',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # time iteration(s)
    for time_step in alg_time_generic:

        # ------------------------------------------------------------------------------------------------------------------
        ## TIME MANAGEMENT (STEP)
        # time source data
        alg_data_time = select_time_range(
            time_start=time_step,
            time_period=24,
            time_frequency='h')
        start_data_time, end_data_time = alg_data_time[0], alg_data_time[-1]
        period_data_time = len(alg_data_time)

        start_data_time = select_time_format(start_data_time, time_format='%Y-%m-%d %H:%M')
        end_data_time = select_time_format(end_data_time, time_format='%Y-%m-%d %H:%M')
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## SOURCE DATA MANAGEMENT
        alg_cfg_step = alg_cfg_obj.fill_obj_from_lut(
            resolve_time_placeholders=True, when=time_step,
            time_keys=('file_time_source', 'path_time_source'),
            extra_tags={
                'file_time_source': time_step, "path_time_source": time_step},
            section=alg_cfg_application, in_place=False,
            template_keys=('file_time_destination','path_time_destination')
        )
        # view application section
        alg_cfg_obj.view(section=alg_cfg_step, table_name='application [cfg step]', table_print=view_table)
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## ORGANIZE AND RUN DATASET 1 (RAIN_EFF)
        # organize source handler rain eff - workflow dset 1
        src_handler_dset_01 = DataLocal(
            path=alg_cfg_step['data_source']['dset_01']['path'],
            file_name=alg_cfg_step['data_source']['dset_01']['file_name'],
            file_type='grid_2d', file_format='geotiff', file_mode='local', file_io='input',
            file_variable=['rain_eff'],
            variable_template={
                "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
                'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
                "vars_data": {"rain_eff": "rain_eff"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle)

        # organize destination handler rain eff - workflow dset 1
        dst_handler_dset_01 = DataLocal(
            path=alg_cfg_step['data_destination']['dset_01']['path'],
            file_name=alg_cfg_step['data_destination']['dset_01']['file_name'],
            time_signature='start',
            file_format='netcdf', file_type='grid_3d_itwater', file_mode='local', file_io='output',
            file_variable=alg_cfg_step['data_destination']['dset_01']['variable'],
            variable_template={
                "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
                "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
                "vars_data": alg_cfg_step['data_destination']['dset_01']['vars_data']
            },
            time_period=24, time_format='%Y%m%d%H%M',
            logger=logging_handle)

        # orchestrator multi time(s) settings - workflow dset 1
        orc_process_01 = Orchestrator.multi_time(
            data_package_in=[src_handler_dset_01], data_package_out=[dst_handler_dset_01],
            data_ref=geo_data,
            configuration=alg_cfg_workflow['WORKFLOW_DSET_01'],
            logger = logging_handle
        )

        # orchestrator multi time(s) execution - workflow dset 1
        orc_process_01.run(
            time=pd.date_range(start_data_time, end_data_time, freq='h'), group='by_time')
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## ORGANIZE AND RUN DATASET 2 (SNOW_MASK)
        # organize source handler snow mask - workflow dset 2
        src_handler_dset_02 = DataLocal(
            path=alg_cfg_step['data_source']['dset_02']['path'],
            file_name=alg_cfg_step['data_source']['dset_02']['file_name'],
            file_type='grid_2d', file_format='geotiff', file_mode='local', file_io='input',
            file_variable=['snow_mask'],
            variable_template={
                "dims_geo": {"X": "longitude", "Y": "latitude", "time": "time"},
                'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
                "vars_data": {"snow_mask": "snow_mask"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle)

        # organize destination handler snow mask - workflow dset 2
        dst_handler_dset_02 = DataLocal(
            path=alg_cfg_step['data_destination']['dset_02']['path'],
            file_name=alg_cfg_step['data_destination']['dset_02']['file_name'],
            time_signature='start',
            file_format='netcdf', file_type='grid_3d_itwater', file_mode='local', file_io='output',
            file_variable=alg_cfg_step['data_destination']['dset_02']['variable'],
            variable_template={
                "dims_geo": {"longitude": "longitude", "latitude": "latitude"},
                "vars_geo": {"longitude": "longitude", "latitude": "longitude"},
                "vars_data": alg_cfg_step['data_destination']['dset_02']['vars_data']
            },
            time_period=24, time_format='%Y%m%d%H%M',
            logger=logging_handle)

        # orchestrator multi time(s) settings - workflow dset 2
        orc_process_02 = Orchestrator.multi_time(
            data_package_in=[src_handler_dset_02], data_package_out=[dst_handler_dset_02],
            data_ref=geo_data,
            configuration=alg_cfg_workflow['WORKFLOW_DSET_02'],
            logger = logging_handle
        )

        # orchestrator multi time(s) execution - workflow dset 2
        orc_process_02.run(
            time=pd.date_range(start_data_time, end_data_time, freq='h'), group='by_time')
        # ------------------------------------------------------------------------------------------------------------------

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
    # run script
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
