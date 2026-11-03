#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN

__date__ = '20261102'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_rfarm_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
TIME_PERIOD=72;
TIME_START='2026-10-31 00:00';
TIME_END='2026-10-31 00:00';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_marche/rfarm/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_marche/rfarm/data/icon-2i/';
PATH_DST='/home/fabio/Desktop/shybox/exec/case_study_marche/rfarm/data/icon-2i/';
PATH_LOG='/home/fabio/Desktop/shybox/exec/case_study_marche/rfarm/log/';
PATH_TMP='/home/fabio/Desktop/shybox/exec/case_study_marche/rfarm/tmp/';

# Set env variables to use proj 8.0.0 or proj 6.0.0
PROJ_DATA=/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/conda/envs/shybox_geo_libraries/share/proj;
PROJ_LIB=

Version(s):
20261102 (1.0.0) --> Release for shybox package (rfarm-nwp base configuration)
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

import pandas as pd

from shybox.time_toolkit.lib_utils_time import (select_time_range, select_time_format)

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_rfarm_toolkit.rfarm_adapter import exec_rfarm_nwp
from shybox.processing_grid_toolkit.lib_proc_mask import mask_data_by_ref
from shybox.processing_grid_toolkit.lib_proc_compute_rain import convert_rain_units

from shybox.orchestrator_toolkit.orchestrator_handler_config import OrchestratorConfig as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_temporary import DataTemporary
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for disaggregating datasets by rainfarm model'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-11-02'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main function
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # crete configuration object
    alg_cfg_obj = ConfigManager.from_source(
        alg_args_file,
        root_key="configuration",
        auto_validate=True, auto_fill_lut=True,
        flat_variables=True, flat_key_mode='value')
    # view lut section
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=view_table)

    # get application section
    alg_cfg_application = alg_cfg_obj.get_section(section='application')
    # fill application section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application,
        resolve_time_placeholders=False, time_keys=('time_start', 'time_end', 'time_period'),
        template_keys=('file_time_destination',)
    )
    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=view_table)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow')
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_cfg_application['log']['path'],
        logger_file=alg_cfg_application['log']['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=['file', 'stream'],
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_rfarm_icon",
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
    alg_reference_time = select_time_range(
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
        file_type='grid_2d', file_format='ascii', file_mode='local',
        file_variable='terrain', file_io='input', file_workflow='GEO',
        variable_template={
            "dims_geo": {"x": "longitude", "y": "latitude"},
            "vars_geo": {"x": "longitude", "y": "latitude"}
        },
        time_signature=None, time_direction=None,
        logger=logging_handle, message=False
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME MANAGEMENT (STEP)
    # time dataset info
    time_data_length = int(alg_cfg_application['time']['dataset'])
    time_data_start = select_time_format(alg_reference_time, time_format='%Y-%m-%d %H:%M')
    # time analysis info
    time_anls_length = int(alg_cfg_application['time']['dataset'])
    time_anls_period = select_time_range(
        time_start=alg_reference_time, time_period=time_anls_length, time_frequency='h')
    time_anls_start = select_time_format(time_anls_period[0], time_format='%Y-%m-%d %H:%M')
    time_anls_end = select_time_format(time_anls_period[-1], time_format='%Y-%m-%d %H:%M')

    # update the applications obj
    alg_cfg_application = alg_cfg_obj.fill_section_with_times(
        alg_cfg_application,
        time_values={
            'file_time_destination': "",
            'path_time_source': alg_reference_time, 'file_time_source': alg_reference_time,
            'path_time_destination': alg_reference_time, 'path_time_run': alg_reference_time}
    )

    # view application section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [time info]', table_print=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## DATASETS MANAGEMENT
    # ancillary ensemble storage (one member per file/store)
    ancillary_data = DataTemporary(
        folder_name=alg_cfg_application['data_ancillary']['path'],
        file_name=alg_cfg_application['data_ancillary']['file_name'],
        writer_type=alg_cfg_application['data_ancillary'].get('writer_type', 'pickle'),
        prefix=alg_cfg_application['data_ancillary'].get('prefix', 'rfarm_'),
    )
    for process in alg_cfg_workflow['process_list']['RFARM_WORKFLOW']:
        if process['function'] == 'exec_rfarm_nwp':
            process.setdefault('args', {})['ancillary_data'] = ancillary_data

    # source data handler
    source_data = DataLocal(
        path=alg_cfg_application['data_source']['RAIN']['path'],
        file_name=alg_cfg_application['data_source']['RAIN']['file_name'],
        file_type='grid_3d', file_format='grib', file_mode='local',
        file_variable='RAIN', file_io='input', file_workflow='RFARM_WORKFLOW',
        variable_template={
            "dims_geo": {"longitude": "longitude", "latitude": "latitude", "step": "time"},
            "vars_data": {"tp": "rain"},
            "units": {"tp": "kg m**-2"}
        },
        time_signature='period',
        time_reference=time_data_start, time_period=time_data_length, time_freq='h', time_direction='forward',
        logger=logging_handle, message=False, tag='source data'
    )

    # destination data handler
    destination_data = DataLocal(
        path=alg_cfg_application['data_destination']['path'],
        file_name=alg_cfg_application['data_destination']['file_name'],
        file_format='netcdf', file_type='grid_rfarm_nwp', file_mode='local',
        file_variable='RAIN', file_io='output', file_workflow='RFARM_WORKFLOW',
        variable_template={
            "dims_geo": {"longitude": "west_east", "latitude": "south_north", "time": "time"},
            "coord_geo": {"Longitude": "longitude", "Latitude": "latitude"},
            "vars_data": {"rain": "Rain"},
            "units": {"rain": "mm"},
        },
        time_period=time_data_length, time_format='%Y%m%d%H%M', time_signature='start',
        logger=logging_handle, message=False, tag='destination data'
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## ORCHESTRATOR MANAGEMENT
    # orchestrator settings
    orc_process = Orchestrator.configure(
        data_package_in={"RAINFARM_SRC": source_data},
        data_package_out={"RAINFARM_DST": destination_data},
        data_ref=geo_data,
        priority=['RFARM_WORKFLOW'],
        configuration=alg_cfg_workflow,
        tag_orchestrator='rainfarm', tag_logger='OrchestratorGrid',
        logger=logging_handle
    )
    # orchestrator exec
    orc_process.run(time=pd.date_range(start=time_anls_start, end=time_anls_end, freq='h'), group='by:file')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    alg_time_elapsed = round(time.time() - start_time, 1)

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to update time reference
def update_time_reference(
        time_period_start, time_period_end, time_start_anls, time_end_anls):

    # Update period start
    if time_period_start != time_start_anls:
        new_start = max(time_period_start, time_start_anls)

        if new_start >= time_end_anls:
            raise ValueError(
                f"Invalid update: start {new_start} is not < analysis end {time_end_anls}"
            )
        time_period_start = new_start

    # Update period end
    if time_period_end != time_end_anls:
        new_end = min(time_period_end, time_end_anls)

        if new_end <= time_start_anls:
            raise ValueError(
                f"Invalid update: end {new_end} is not > analysis start {time_start_anls}"
            )
        time_period_end = new_end

    return time_period_start, time_period_end
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":
    # run script
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
