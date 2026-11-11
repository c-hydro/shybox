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

Dataset sources:
Observed: all variables from PATH_OBS for both run modes.
Forecast deterministic: all variables from ECMWF in PATH_FRC_NWP.
Forecast probabilistic: rain members from PATH_FRC_RFARM; other variables from PATH_FRC_NWP.
Time windows and shared obs/nwp/rfarm folder/file formats are declared in LUT/format/template.
Interpolation and splitting are optional per-variable operations; all times are UTC.
This entry point configures workflows without reading arrays or writing forcing.
Use --dry-run --plan-files for a JSON planning/setup report. Python callers can
run the returned orchestrators explicitly when the datasets are ready.

Version(s):
20261109 (1.0.0) --> HMC/S3M observed/forecast forcing preparation with ensemble support
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import json
import logging
import os
import time
from copy import deepcopy
from importlib import import_module
from pathlib import Path

import pandas as pd

from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
from shybox.logging_toolkit.logging_handler import LoggingManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for preparing HMC/S3M forcing datasets'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-11-09'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def configure_dataset_workflows(planner, configuration, *, logger=None):
    """Configure NWP-style DataLocal pairs and one orchestrator per member/time.

    Every canonical variable uses its VARIABLE_WORKFLOW process list. All
    variable workflows for an output step share an orchestrator, so fields
    destined for the same file are assembled together. Return
    {member: {validity_time: {source, destination, orchestrator}}} without
    reading arrays or running processes. Use orchestrator.run(time=[stamp])
    explicitly when ready to process the configured files.
    """
    from shybox.dataset_toolkit.dataset_handler_local import DataLocal
    from shybox.orchestrator_toolkit.orchestrator_handler_config import OrchestratorConfig as Orchestrator
    from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes import PROCESSES

    if not isinstance(configuration, dict) or not isinstance(configuration.get('process_list'), dict):
        raise ValueError('workflow.process_list must map VARIABLE_WORKFLOW names to process lists.')
    configuration = deepcopy(configuration)
    log = logger if logger is not None else planner.log
    files = planner.file_plan
    process_lists = configuration['process_list']
    workflow_names = {variable: f'{variable.upper()}_WORKFLOW' for variable in files.variable.unique()}
    for variable, name in workflow_names.items():
        if not isinstance(process_lists.get(name), list) or not process_lists[name]:
            raise ValueError(f'{variable}: workflow.process_list.{name} must contain at least one process.')

    # Import the same process families used by the NWP converters to register them.
    modules = {
        'exec_interpolate_data_generic': 'grids_adapter',
        'mask_data_by_ref': 'lib_proc_mask', 'mask_data_by_limits': 'lib_proc_mask',
        'convert_temperature_units': 'lib_proc_compute_temperature',
        'compute_data_wind_speed': 'lib_proc_compute_wind',
        'compute_data_rh': 'lib_proc_compute_humidity',
        'compute_data_astronomic_radiation': 'lib_proc_compute_radiation',
    }
    for name in workflow_names.values():
        for process in process_lists[name]:
            function = process.get('function')
            if function not in PROCESSES and function in modules:
                import_module('shybox.processing_grid_toolkit.' + modules[function])
            if function not in PROCESSES:
                raise ValueError(f'{name}: unregistered process {function!r}.')

    options = configuration.setdefault('options', {})
    options.setdefault('io_config', {'mode': 'many_data_files_to_one_file'})
    options.setdefault('intermediate_output', 'Memory')
    source_settings = {
        group: {variable: cfg for cfg in sources.values() for variable in cfg['variable'].values()}
        for group, sources in planner.application['data_source'].items()
    }
    destination_settings = planner.application['data_destination']
    destinations_by_group = bool(set(destination_settings) & {'observed', 'forecast'})

    geo_data = None
    geo = planner.application.get('geo')
    if geo and len(files):
        geo = geo.get('terrain', geo)
        path = Path(planner.fill_file_name(geo, files.time.iloc[0], source=False))
        geo_data = DataLocal(
            path=str(path.parent), file_name=path.name,
            data_role='auxiliary', file_type='grid_2d', file_format=geo.get('format', 'ascii'),
            file_mode='local', file_variable='terrain', file_workflow='GEO', file_io='input',
            variable_template=geo.get('variable_template', {
                'dims_geo': {'x': 'longitude', 'y': 'latitude'},
                'vars_geo': {'x': 'longitude', 'y': 'latitude'},
            }),
            time_signature=None, time_direction=None, logger=log, message=False,
        )

    # Use the planner's member expansion, including shared deterministic sources.
    sources_by_member = planner.file_by_ensemble('source')
    destinations_by_member = planner.file_by_ensemble('destination')
    frequency = planner.time_plan.frequency
    result = {}
    for member, times in sources_by_member.items():
        result[member] = {}
        for time_key, source_paths in times.items():
            stamp = pd.Timestamp(time_key)
            rows = files.loc[files.time.eq(stamp)]
            member_rows = rows.loc[rows.output_member.eq(member) | rows.ensemble_member.eq(member)]
            if not member_rows.empty:
                rows = pd.concat([member_rows, rows.loc[
                    rows.output_member.isna() & rows.ensemble_member.isna()]])
            rows = rows.drop_duplicates('variable')
            data_in, data_out, active_workflows = {}, {}, {}
            for row in rows.itertuples(index=False):
                if source_paths[row.variable] is None:
                    raise ValueError(f'{member}.{time_key}.{row.variable}: no source configured for workflow setup.')
                cfg = source_settings[row.group][row.variable]
                collections = destination_settings[row.group] if destinations_by_group else destination_settings
                if 'file_name' in collections:
                    collections = {'collection': collections}
                dst = collections[row.collection]
                name = workflow_names[row.variable]
                source_id, destination_id = row.variable.upper() + '_DATA_SRC', row.variable.upper() + '_DATA_DST'
                source_path = Path(source_paths[row.variable])
                destination_path = Path(destinations_by_member[member][time_key][row.variable])
                input_template = deepcopy(cfg.get('variable_template', {
                    'dims_geo': {cfg['x']: 'longitude', cfg['y']: 'latitude', cfg['time']: 'time'},
                    'vars_data': {row.source_name: row.variable},
                }))
                input_template['vars_data'] = {row.source_name: row.variable}
                output_template = deepcopy(dst.get('variable_template', {
                    'dims_geo': {'longitude': 'west_east', 'latitude': 'south_north', 'time': 'time'},
                    'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
                }))
                output_template['vars_data'] = {row.variable: row.output_name}
                output_template['units'] = {row.variable: dst.get('units', {}).get(row.variable, 'NA')}
                output_type = {
                    'hmc': 'grid_hmc', 'hmc_forcing': 'grid_hmc',
                    's3m': 'grid_s3m', 's3m_forcing': 'grid_s3m',
                }.get(dst['type'], 'grid_' + dst['type'])
                data_in[source_id] = DataLocal(
                    path=str(source_path.parent), file_name=source_path.name,
                    file_type=cfg.get('file_type', 'grid_' + cfg['dims']), file_format=cfg['format'],
                    file_mode='local', file_variable=source_id, file_workflow=name, file_io='input',
                    variable_template=input_template, file_args=deepcopy(cfg.get('file_args', {})),
                    data_mandatory=cfg.get('required', False),
                    time_signature='period' if cfg['dims'] == '3d' else 'step',
                    time_reference=stamp, time_period=1, time_freq=frequency,
                    time_direction='forward', logger=log, message=False,
                )
                data_out[destination_id] = DataLocal(
                    path=str(destination_path.parent), file_name=destination_path.name,
                    file_type=dst.get('file_type', output_type), file_format=dst['format'],
                    file_mode='local', file_variable=destination_id, file_workflow=name, file_io='output',
                    variable_template=output_template, file_args=deepcopy(dst.get('file_args', {})),
                    time_signature='step', time_reference=stamp, time_period=1,
                    time_freq=frequency, time_format='%Y%m%d%H%M',
                    time_direction='forward', logger=log, message=False,
                )
                active_workflows[name] = process_lists[name]
            selected_configuration = {**configuration, 'process_list': active_workflows}
            orchestrator = Orchestrator.configure(
                data_package_in=data_in, data_package_out=data_out, data_ref=geo_data,
                configuration=selected_configuration, logger=log,
                priority=[name for name in configuration.get('priority', []) if name in active_workflows],
                tag_orchestrator=f'datasets_{member}_{stamp:%Y%m%d%H%M}', tag_logger='OrchestratorGrid',
            )
            result[member][time_key] = {
                'source': data_in, 'destination': data_out, 'orchestrator': orchestrator,
            }
    return result


def main(view_table: bool = False):

    # ------------------------------------------------------------------------------------------------------------------
    ## SETTINGS MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(settings_folder=os.path.dirname(os.path.realpath(__file__)))
    parsed = alg_args_obj.get()
    alg_args_file, alg_args_time = parsed[:2]
    extra = parsed[2] if len(parsed) == 3 else {}

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
    planning_options = {
        'n_step_to_check_obs': 3, 'n_step_to_check_frc': 1,
        'flag_adjust_obs_frc': True, 'rules_to_fill_obs': 'fill_using_frc',
        'rules_to_fill_frc': None,
    }
    planning_options.update(alg_cfg_application.get('planning', {}))
    alg_cfg_planner.update_time(**planning_options)
    # get file planning
    files_planning = alg_cfg_planner.file_plan

    # unique files, with the variables and times using each file.
    files_summary_source = alg_cfg_planner.file_table(kind="source")
    # organize source files by ensemble(s)
    files_by_ensemble_source = alg_cfg_planner.file_by_ensemble(kind='source')
    # organize destination files by ensemble(s)
    files_by_ensemble_destination = alg_cfg_planner.file_by_ensemble(kind='destination')
    # ------------------------------------------------------------------------------------------------------------------


    workflows = configure_dataset_workflows(alg_cfg_planner, alg_cfg_workflow, logger=logging_handle)
    logging_handle.info(
        f'Configured dataset workflows for {len(workflows)} members and '
        f'{sum(len(times) for times in workflows.values())} member/time steps')


    # ------------------------------------------------------------------------------------------------------------------
    ## INFO END
    elapsed = round(time.monotonic() - started, 1)
    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')',
                               blank_before=True)
    logging_handle.info_header('TIME ELAPSED: ' + str(elapsed) + ' seconds')
    logging_handle.info_header('... END')
    logging_handle.info_header(LoggingManager.rule_line('=', 78))
    if extra.get('dry-run') or extra.get('plan-files'):
        summary = alg_cfg_planner.time_plan.as_dict()
        summary.update({
            'source_files': int(files_summary_source.file_name.nunique()),
            'destination_files': int(alg_cfg_planner.file_table('destination').file_name.nunique()),
            'source_files_available': int(alg_cfg_planner.file_available().file_name.nunique()),
            'source_files_missing': int(alg_cfg_planner.file_not_available().file_name.nunique()),
            'destination_files_missing': int(alg_cfg_planner.file_not_available('destination').file_name.nunique()),
            'configured_workflow_steps': sum(len(times) for times in workflows.values()),
            'configured_members': list(workflows),
        })
        if extra.get('plan-files'):
            summary['file_plan'] = files_planning.astype(object).where(
                pd.notna(files_planning), None).to_dict(orient='records')
        print(json.dumps(summary, indent=2, default=str))
    return workflows
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == '__main__':
    main(view_table=True)
# ----------------------------------------------------------------------------------------------------------------------
