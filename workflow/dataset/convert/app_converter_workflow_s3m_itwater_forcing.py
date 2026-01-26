#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW CONVERTER BASE [S3M]

__date__ = '20260123'
__version__ = '1.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
TIME_START='1981-01-07';
TIME_END='1981-01-08';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_itwater/converter_s3m/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_itwater/converter_s3m/data/';
PATH_DST='/home/fabio/Desktop/shybox/exec/case_study_itwater/converter_s3m/data/';
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_itwater/converter_s3m/log/;
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_itwater/converter_s3m/tmp/

Version(s):
20260123 (1.1.0) --> Release for shybox package (hmc datasets converter base configuration)
20250310 (1.0.0) --> Beta release for shybox package (s3m datasets converter base configuration)
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
from shybox.processing_toolkit.lib_proc_interp import interpolate_data
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref

from shybox.time_toolkit.lib_utils_time import select_time_range, select_time_format
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets - Converter'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2026-01-23'
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
        template_keys=('path_destination_time', 'file_destination_time')
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
        name="shybox_algorithm_converter_by_domain_s3m",
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

        # dataset handler rain
        src_handler_rain = DataLocal(
            path=alg_cfg_step['data_source']['RAIN']['path'],
            file_name=alg_cfg_step['data_source']['RAIN']['file_name'],
            file_type='grid_itwater', file_format='netcdf', file_mode='local',
            file_variable=['RAIN'], file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Rain": "rain"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle
        )

        # dataset handler air_temperature
        src_handler_air_t = DataLocal(
            path=alg_cfg_step['data_source']['AIR_T']['path'],
            file_name=alg_cfg_step['data_source']['AIR_T']['file_name'],
            file_type='grid_itwater', file_format='netcdf', file_mode='local',
            file_variable=['AIR_T'], file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Tair": "air_temperature"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle
        )

        # dataset handler relative_humidity
        src_handler_rh = DataLocal(
            path=alg_cfg_step['data_source']['RH']['path'],
            file_name=alg_cfg_step['data_source']['RH']['file_name'],
            file_type='grid_itwater', file_format='netcdf', file_mode='local',
            file_variable=['RH'], file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"RH": "relative_humidity"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle
        )

        # dataset handler incoming_radiation
        src_handler_inc_rad = DataLocal(
            path=alg_cfg_step['data_source']['INC_RAD']['path'],
            file_name=alg_cfg_step['data_source']['INC_RAD']['file_name'],
            file_type='grid_itwater', file_format='netcdf', file_mode='local',
            file_variable=['INC_RAD'], file_io='input',
            variable_template={
                "dims_geo": {"lon": "longitude", "lat": "latitude", "nt": "time"},
                "vars_data": {"Rad": "incoming_radiation"}
            },
            time_signature='period',
            time_reference=start_data_time, time_period=period_data_time, time_freq='h', time_direction='forward',
            logger=logging_handle
        )

        ## DESTINATION DATA MANAGEMENT
        dst_handler = DataLocal(
            path=alg_cfg_step['data_destination']['path'],
            file_name=alg_cfg_step['data_destination']['file_name'],
            file_type='grid_s3m', file_variable=['RAIN', 'AIR_T', 'RH', 'INC_RAD'],
            file_format='netcdf', file_mode='local', file_io='output',
            variable_template={
                "dims_geo": {"longitude": "X", "latitude": "Y", "time": "time"},
                "coord_geo": {"longitude": "X", "latitude": "Y"},
                "vars_data": {
                    "rain": "Rain",
                    "air_temperature": "AirTemperature",
                    "relative_humidity": "RelHumidity",
                    "incoming_radiation": "IncRadiation"}
            },
            time_signature='step', time_period=1, time_format='%Y%m%d%H%M',
            logger=logging_handle, message=False)
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## ORCHESTRATOR MANAGEMENT
        # orchestrator settings
        orc_process = Orchestrator.multi_variable(
            data_package_in=[src_handler_rain, src_handler_air_t, src_handler_rh, src_handler_inc_rad],
            data_package_out=dst_handler,
            data_ref=geo_data,
            priority=['air_temperature'],
            configuration=alg_cfg_workflow,
            logger=logging_handle
        )
        # orchestrator exec
        orc_process.run(time=pd.date_range(start=start_data_time, end=end_data_time, freq='h'))

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
