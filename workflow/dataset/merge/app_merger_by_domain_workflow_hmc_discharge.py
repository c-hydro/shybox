#!/usr/bin/python3
"""
SHYBOX PACKAGE - APP PROCESSING DATASET MAIN - MERGER BY DOMAIN - DISCHARGE

__date__ = '20260122'
__version__ = '1.1.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Francesco Avanzi (francesco.avanzi@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_merger_workflow_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='italy';
TIME_START="'1983-10-31 11:00'";
TIME_END="'1983-10-31 12:00'";
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_destine/merger_hmc_by_domain/geo/';
PATH_SRC='/home/fabio/Desktop/shybox/dset/case_study_destine/merger_hmc_by_domain/data/';
PATH_DST='/home/fabio/Desktop/shybox/exec/case_study_destine/merger_hmc_by_domain/data';
PATH_LOG=$HOME/Desktop/shybox/exec/case_study_destine/merger_hmc_by_domain/log/;
PATH_TMP=$HOME/Desktop/shybox/exec/case_study_destine/merger_hmc_by_domain/tmp/

Version(s):
20260129 (1.2.0) --> Add class method to create data on demand
20260122 (1.1.0) --> Refactor using class methods in shybox package
20250403 (1.0.0) --> Beta release for shybox package
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager

from shybox.orchestrator_toolkit.orchestrator_handler_grid import OrchestratorGrid as Orchestrator
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.dataset_toolkit.dataset_handler_on_demand import DataOnDemand
from shybox.logging_toolkit.logging_handler import LoggingManager

# fx imported in the PROCESSES (will be used in the global variables PROCESSES) --> DO NOT REMOVE
from shybox.processing_toolkit.lib_proc_mask import mask_data_by_ref
from shybox.processing_toolkit.lib_proc_merge import merge_data_by_ref
from shybox.processing_toolkit.lib_proc_merge import merge_data_by_watermark

from shybox.time_toolkit.lib_utils_time import select_time_range, select_time_format
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Application for processing datasets - Merger by Domain - Discharge'
alg_type = 'Package'
alg_version = '1.1.0'
alg_release = '2026-01-22'
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
        name="shybox_algorithm_merger_by_domain_hmc",
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
    ref_data = DataOnDemand(
        info=alg_cfg_application['geo']['grid'],
        file_type='grid_2d', file_format=None, file_mode='local', file_variable='grid', file_io='input',
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

        # iterate over src datasets
        dset_src_handler_list = []
        for alg_data_id, (alg_data_src_key, alg_data_src_settings) in enumerate(alg_cfg_step['data_source'].items()):

            # deps handler
            dset_src_handler_deps = DataLocal(
                path=alg_data_src_settings['file_watermark']['path'],
                file_name=alg_data_src_settings['file_watermark']['file_name'],
                file_type='grid_2d', file_format='ascii', file_mode='local',
                file_variable=['WATERMARK'], file_io='input',
                data_id=alg_data_id,
                variable_template={
                    "dims_geo": {"x": "longitude", "y": "latitude"},
                    "vars_data": {"watermark": "deps_watermark"}
                },
                time_signature=None, time_direction=None,
                logger=logging_handle
            )

            # dataset handler
            dset_src_handler_data = DataLocal(
                path=alg_data_src_settings['file_data']['path'],
                file_name=alg_data_src_settings['file_data']['file_name'],
                file_type='grid_hmc', file_format='netcdf', file_mode='local',
                file_variable=['DISCHARGE'], file_io='input',
                file_deps={'watermark': dset_src_handler_deps},
                data_id=alg_data_id,
                variable_template={
                    "dims_geo": {"west_east": "longitude", "south_north": "latitude", "time": "time"},
                    "vars_data": {"SM": "simulated_discharge"}
                },
                time_signature='current',
                time_reference=time_step, time_period=1, time_freq='h', time_direction='forward',
                logger=logging_handle
            )

            # append to src list
            dset_src_handler_list.append(dset_src_handler_data)

        ## DESTINATION DATA MANAGEMENT
        # dataset handler 'grid_2d' or 'grid' for geotiff
        dset_dst_handler_obj = DataLocal(
            path=alg_cfg_step['data_destination']['path'],
            file_name=alg_cfg_step['data_destination']['file_name'],
            file_type='grid_2d', file_format='geotiff', file_mode='local',
            file_variable=['DISCHARGE'], file_io='output',
            variable_template={
                "dims_geo": alg_cfg_step['data_destination']['dims_geo'],
                "vars_data": alg_cfg_step['data_destination']['vars_data']
            },
            time_signature='current',
            time_reference=time_step, time_period=1, time_freq='h', time_direction='forward',
        )
        # ------------------------------------------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------------------------------------------
        ## ORCHESTRATOR MANAGEMENT
        # orchestrator settings
        orc_process = Orchestrator.multi_tile(
            data_package_in=dset_src_handler_list,
            data_package_out=dset_dst_handler_obj,
            data_ref=ref_data,
            priority=['simulated_discharge'],
            configuration=alg_cfg_workflow,
            logger=logging_handle
        )
        # orchestrator exec
        orc_process.run(time=time_step)
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
