#!/usr/bin/env python3
"""
SHYBOX - Snow HYdro toolBOX - WORKFLOW RUNNER DATASETS - HMC/S3M

__date__ = '20261109'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python app_runner_workflow_hmc_datasets_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Examples of environment variables declarations:
DOMAIN_NAME='marche';
PATH_GEO='/home/fabio/Desktop/shybox/dset/case_study_marche/runner_hmc_datasets/geo/';
PATH_OBS='/home/fabio/Desktop/shybox/dset/case_study_marche/runner_hmc_datasets/data/obs/';
PATH_FRC_NWP='/home/fabio/Desktop/shybox/dset/case_study_marche/runner_hmc_datasets/data/nwp/';
PATH_FRC_RFARM='/home/fabio/Desktop/shybox/dset/case_study_marche/runner_hmc_datasets/data/rfarm/';
PATH_DST='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/exec/case_study_marche/runner_hmc_datasets/forcing/';
PATH_LOG='/home/fabio/Documents/Work_Area/Code_Development/Workspace/shybox/exec/case_study_marche/runner_hmc_datasets/log/';

Example of deterministic datasets preparation (from the repository root):
python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
    -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
    -time "2026-11-09 09:35" --dry-run

Example of probabilistic datasets preparation (from the repository root):
python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
    -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
    -time "2026-11-09 09:35" --dry-run

Version(s):
20261109 (1.0.0) --> HMC/S3M observed/forecast forcing preparation with ensemble support
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import time
from copy import deepcopy

import pandas as pd

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.orchestrator_toolkit.orchestrator_handler_config import OrchestratorConfig as Orchestrator
from shybox.orchestrator_toolkit.orchestrator_handler_config import config_process_by_variables
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for preparing HMC/S3M forcing datasets'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-11-13'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorith main
def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # create the SHYBOX configuration object and view its LUT
    alg_cfg_obj = ConfigManager.from_source(
        alg_args_file, root_key='configuration', application_key=None,
        auto_validate=True, auto_fill_lut=True, flat_variables=True, flat_key_mode='value',
        convert_none_to_nan=False, apply_time_template_for_none=False)
    alg_cfg_lut = alg_cfg_obj.get_section(section='lut')
    alg_cfg_obj.view(section=alg_cfg_lut, table_name='lut', table_print=view_table)

    alg_cfg_workflow = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_obj.get_section(section='workflow', raise_if_missing=True),
        resolve_time_placeholders=False)
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow', table_print=view_table)

    ## APPLICATION CONFIGURATION MANAGEMENT
    # get time section
    alg_cfg_application = alg_cfg_obj.get_section(section='application', raise_if_missing=True)
    # fill time section
    alg_cfg_application = alg_cfg_obj.fill_obj_from_lut(
        section=alg_cfg_application, resolve_time_placeholders=False,
        time_keys=(
            'time_run', 'time_frequency', 'time_rounding',
            'time_look_observed', 'time_look_forecast',
            'time_horizon_observed', 'time_horizon_forecast',
            'time_priority_observed', 'time_priority_forecast',
        ))

    # view time section
    alg_cfg_obj.view(section=alg_cfg_application, table_name='application [cfg info]', table_print=view_table)

    # get workflow section
    alg_cfg_workflow = alg_cfg_obj.get_section(section='workflow')
    # view workflow section
    alg_cfg_obj.view(section=alg_cfg_workflow, table_name='workflow', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## TIME CONFIGURATION MANAGEMENT
    # Reuse the resolved application section; this works with published ConfigManager.
    alg_cfg_time = deepcopy(alg_cfg_application['time'])

    # view time section
    alg_cfg_obj.view(section=alg_cfg_time, table_name='time [cfg info]', table_print=view_table)
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
        name="shybox_runner_datasets",
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
    started = time.monotonic()
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
    ## DATASETS MANAGEMENT
    time_tags = DatasetPlanner.create_time_tags(
        template=alg_cfg_obj.get_section(section='template', raise_if_missing=True),
        formats=alg_cfg_obj.get_section(section='format', raise_if_missing=True))
    alg_cfg_planner = DatasetPlanner.from_config(
        info=alg_cfg_application['info'], time=alg_cfg_time,
        data_src_obs=alg_cfg_application['data_source']['observed'],
        data_src_frc=alg_cfg_application['data_source']['forecast'],
        data_dst_obs=alg_cfg_application['data_destination'],
        data_dst_frc=alg_cfg_application['data_destination'],
        time_run=alg_args_time, geo=alg_cfg_application.get('geo'),
        time_tags=time_tags, logger=logging_handle)

    print(alg_cfg_planner.variables)
    print(alg_cfg_planner.time_table)
    print(alg_cfg_planner.mode, alg_cfg_planner.members)

    # check only the requested window and fill the trailing observed gap.
    planner_cfg_options = {
        'n_step_to_check_obs': 3, 'n_step_to_check_frc': 1,
        'flag_adjust_obs_frc': True, 'rules_to_fill_obs': 'fill_using_frc',
        'rules_to_fill_frc': None,
        'reset_history' : False, 'flag_history': True, 'pickle_history': "plans_file.workspace",
    }
    planner_cfg_options.update(alg_cfg_application.get('planning', {}))
    alg_cfg_planner.update_time(**planner_cfg_options)
    # get file planning
    files_planning = alg_cfg_planner.file_plan

    # unique files, with the variables and times using each file.
    files_summary_source = alg_cfg_planner.file_table(kind="source")
    # organize source files by ensemble(s)
    files_by_ensemble_source = alg_cfg_planner.file_by_ensemble(kind='source')
    # organize destination files by ensemble(s)
    files_by_ensemble_destination = alg_cfg_planner.file_by_ensemble(kind='destination')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## PROCESS MANAGEMENT
    # Discover and register processes required by the planned variables.
    process_lists = alg_cfg_workflow['process_list']
    process_locations = alg_cfg_workflow.get('process_locations', [])
    variable_lists = files_planning.variable.unique()

    # method to configure processes by variable(s)
    workflow_by_variable = config_process_by_variables(
        process_list_name=process_lists, process_list_locations=process_locations,
        variable_list=variable_lists, workflow_template='{variable}_WORKFLOW_FRC',)

    # iterate over ensemble
    collections_execution = {}
    for (ensemble_member_src, ensemble_data_src), (ensemble_member_dst, ensemble_data_dst) in zip(
            files_by_ensemble_source.items(), files_by_ensemble_destination.items()):

        # check ensemble member
        assert ensemble_member_src == ensemble_member_dst, 'ensemble source and destination are not the same'
        ensemble_member = ensemble_member_src = ensemble_member_dst

        # iterate over times
        collections_src, collections_dst = {}, {}
        for id_time, ( (ensemble_time_src, ensemble_variable_src), (ensemble_time_dst, ensemble_variable_dst) ) in (
                enumerate(zip(ensemble_data_src.items(), ensemble_data_dst.items()))):

            assert ensemble_time_src == ensemble_time_dst, 'time source and destination are not the same'
            ensemble_time = pd.Timestamp(ensemble_time_src)

            # iterate in source variables
            for variable_name_src, variable_path_src in ensemble_variable_src.items():

                # define variable tag
                variable_tag_src = f"{ensemble_member}:{variable_name_src}_{ensemble_time:%Y%m%d%H%M}_src"

                # get variable settings
                variable_settings_src = alg_cfg_application['data_source']['forecast'][variable_name_src]
                # get variable folder and file_name
                variable_folder_src, variable_file_name_src = os.path.split(variable_path_src)
                # get workflow variable
                workflow_variable_src = workflow_by_variable[variable_name_src]

                # instantiate data local source
                data_src = DataLocal(
                    path=variable_folder_src, file_name=variable_file_name_src,
                    file_type='grid_3d', file_format='netcdf', file_mode='local',
                    file_variable=variable_name_src, file_io='input', file_workflow=workflow_variable_src,
                    variable_template={
                        "dims_geo": {
                            "longitude": variable_settings_src['x'], "latitude": variable_settings_src['y'],
                            "time": variable_settings_src['time'],
                        },
                        "vars_data": variable_settings_src['variable']
                    },
                    time_signature='current',
                    time_reference=ensemble_time, time_period=1, time_freq='h', time_direction='forward',
                    logger=logging_handle, message=False, tag=variable_tag_src
                )

                # define collection src
                collections_src[variable_tag_src] = data_src

            # iterate in destination variables
            for variable_name_dst, variable_path_dst in ensemble_variable_dst.items():

                # Define variable tag and select its destination collection settings.
                variable_tag_dst = f"{ensemble_member}:{variable_name_dst}_{ensemble_time:%Y%m%d%H%M}_dst"

                # get variable settings
                #variable_settings_dst = alg_cfg_application['data_destination']['forecast'][variable_name_dst]
                variable_settings_dst = alg_cfg_application['data_destination']['collections']
                # get variable folder and file_name
                variable_folder_dst, variable_file_name_dst = os.path.split(variable_path_dst)
                # get workflow variable
                workflow_variable_dst = workflow_by_variable[variable_name_dst]

                # instantiate data local destination.
                data_dst = DataLocal(
                    path=variable_folder_dst, file_name=variable_file_name_dst,
                    file_type='netcdf',
                    file_format='hmc_forcing', file_mode='local',
                    file_variable=variable_name_dst, file_io='output', file_workflow=workflow_variable_dst,
                    variable_template={
                        "dims_geo": {
                            "longitude": variable_settings_dst['x'], "latitude": variable_settings_dst['y'],
                            "time": variable_settings_dst['time'],
                        },
                        "vars_data": {
                            variable_name_dst: variable_settings_dst['variable'][variable_name_dst]
                        },
                        "units": {
                            variable_name_dst: variable_settings_dst['units'][variable_name_dst]
                        },
                    },
                    file_args=deepcopy(variable_settings_dst.get('file_args', {})),
                    time_signature='step',
                    time_reference=ensemble_time, time_period=1, time_freq='h', time_direction='forward',
                    logger=logging_handle, message=False, tag=variable_tag_dst
                )

                # Define collection dst.
                collections_dst[variable_tag_dst] = data_dst

            # selected_configuration = {**configuration, 'process_list': active_workflows}
            # orchestrator configure
            orc_process_ensemble = Orchestrator.configure(
                data_package_in=collections_src,
                data_package_out=collections_dst,
                data_ref=geo_data,
                configuration=alg_cfg_workflow, logger=logging_handle,
                priority=None,
                tag_orchestrator=f'runner_datasets_{ensemble_member}_{ensemble_time:%Y%m%d%H%M}',
                tag_logger='OrchestratorGrid',
            )

            # orchestrator exec
            orc_process_ensemble.run(time=ensemble_time, group='by:step')

            # orchestrator history
            collections_execution[ensemble_member][ensemble_time] = {
                'source': collections_src, 'destination': collections_dst, 'orchestrator': orc_process_ensemble,
            }


    workflows = configure_dataset_workflows(alg_cfg_planner, alg_cfg_workflow, logger=logging_handle)
    logging_handle.info(
        f'Configured dataset workflows for {len(workflows)} members and '
        f'{sum(len(times) for times in workflows.values())} member/time steps')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    # info algorithm (end)
    elapsed = round(time.time() - started, 1)

    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')', blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header('Bye, Bye')
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == '__main__':
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
