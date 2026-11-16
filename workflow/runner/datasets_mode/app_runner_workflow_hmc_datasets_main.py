#!/usr/bin/env python3
"""HMC template with deterministic preparation followed by member updates.

python -m workflow.runner.datasets_mode.app_runner_workflow_hmc_datasets_main \
    -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_probabilistic.json \
    -time "2026-11-09 10:00" --workspace /work/tmp/hmc_modes

Existing sequential runner scripts and settings are not imported or modified.
"""
import argparse
from copy import deepcopy
import logging
from pathlib import Path
import tempfile

import pandas as pd

from shybox.config_toolkit.config_handler import ConfigManager
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.orchestrator_toolkit.orchestrator_handler_config import (
    OrchestratorConfig as Orchestrator, config_process_by_variables,
)
from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
from shybox.runner_toolkit.datasets.lib_utils_mode import (
    DatasetModeStore, file_identity, local_source, publish_member,
)


TIME_KEYS = (
    'time_run', 'time_frequency', 'time_rounding', 'time_look_observed', 'time_look_forecast',
    'time_horizon_observed', 'time_horizon_forecast', 'time_priority_observed', 'time_priority_forecast',
)


def load_configuration(file_name):
    manager = ConfigManager.from_source(
        str(Path(file_name).resolve()), root_key='configuration', application_key=None,
        auto_validate=True, auto_fill_lut=True, flat_variables=True, flat_key_mode='value',
        convert_none_to_nan=False, apply_time_template_for_none=False)
    application = manager.fill_obj_from_lut(
        section=manager.get_section(section='application', raise_if_missing=True),
        resolve_time_placeholders=False, time_keys=TIME_KEYS)
    workflow = manager.fill_obj_from_lut(
        section=manager.get_section(section='workflow', raise_if_missing=True),
        resolve_time_placeholders=False)
    tags = DatasetPlanner.create_time_tags(
        template=manager.get_section(section='template', raise_if_missing=True),
        formats=manager.get_section(section='format', raise_if_missing=True))
    return application, workflow, tags


def selected_workflows(workflow, variables, time_mode):
    """Prefer mode-specific workflows; support the existing HMC forecast template."""
    lists = workflow['process_list']
    names = {}
    suffix = 'OBS' if time_mode == 'observed' else 'FRC'
    for variable in variables:
        candidates = [f'{variable}_WORKFLOW_{suffix}'.upper(), f'{variable}_WORKFLOW'.upper()]
        # The accepted HMC JSON supplies only _FRC pass-through chains.
        if time_mode == 'observed':
            candidates.append(f'{variable}_WORKFLOW_FRC'.upper())
        name = next((name for name in candidates if name in lists), None)
        if name is None:
            raise ValueError(f'{time_mode}.{variable}: configure one of {candidates}.')
        names[variable] = name
    for variable, name in names.items():
        config_process_by_variables(
            process_list_name=lists, process_list_locations=workflow.get('process_locations', []),
            variable_list=[variable], workflow_template=name)
    selected = deepcopy(workflow)
    selected['process_list'] = {name: deepcopy(lists[name]) for name in names.values()}
    return names, selected


def process_step(planner, workflow, geo_data, rows, file_name, workspace, logger):
    """Run just these variables into one plain HMC forcing file, as in the template."""
    stamp = pd.Timestamp(rows.iloc[0]['time'])
    time_mode = rows.iloc[0]['time_mode']
    names, configuration = selected_workflows(workflow, rows.variable.unique(), time_mode)
    sources, destinations = {}, {}
    file_name = Path(file_name)
    file_name.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='step-', dir=workspace) as folder:
        configuration.setdefault('options', {})['tmp_dir'] = folder
        for row in rows.itertuples(index=False):
            source_cfg = planner.source_settings(row.variable, time_mode)
            destination_cfg = planner.destination_settings(row.variable, time_mode)
            source_path = local_source(row.file_source, Path(folder) / 'inputs')
            source_tag = f'{row.variable}_{stamp:%Y%m%d%H%M}_src'
            destination_tag = f'{row.variable}_{stamp:%Y%m%d%H%M}_dst'
            common = dict(
                file_mode='local', file_variable=row.variable, file_workflow=names[row.variable],
                time_reference=stamp, time_period=1,
                time_freq=planner.application['time']['time_frequency'], time_direction='forward',
                logger=logger, message=False)
            sources[source_tag] = DataLocal(
                path=str(source_path.parent), file_name=source_path.name,
                file_type='grid_3d' if source_cfg['dims'] == '3d' else 'grid_2d',
                file_format=source_cfg['format'], file_io='input',
                variable_template={
                    'dims_geo': {'longitude': source_cfg['x'], 'latitude': source_cfg['y'],
                                 'time': source_cfg['time']},
                    'vars_data': {row.source_name: row.variable}},
                time_signature='current', tag=source_tag, **common)
            destinations[destination_tag] = DataLocal(
                path=str(file_name.parent), file_name=file_name.name,
                file_type='hmc_forcing', file_format='netcdf', file_io='output',
                variable_template={
                    'dims_geo': {'longitude': destination_cfg['x'], 'latitude': destination_cfg['y'],
                                 'time': destination_cfg['time']},
                    'vars_data': {row.variable: row.output_name},
                    'units': {row.variable: destination_cfg['units'][row.variable],
                              row.output_name: destination_cfg['units'][row.variable]}},
                file_args={**destination_cfg.get('file_args', {}), 'file_compression': False, 'file_update': True},
                time_signature='step', tag=destination_tag, **common)
        orchestrator = Orchestrator.configure(
            data_package_in=sources, data_package_out=destinations, data_ref=geo_data,
            configuration=configuration, logger=logger, priority=None,
            tag_orchestrator=f'runner_modes_{time_mode}_{stamp:%Y%m%d%H%M}', tag_logger='OrchestratorGrid')
        orchestrator.run(time=stamp, group='by:step')
        # Set unit attributes from the destination schema explicitly. The
        # NetCDF HMC writer accepts both canonical and output names, but
        # collection filtering may leave only one of those names at write time.
        from netCDF4 import Dataset as NetCDFDataset
        with NetCDFDataset(file_name, 'a') as output:
            for row in rows.itertuples(index=False):
                units = planner.destination_settings(row.variable, time_mode)['units'][row.variable]
                output.variables[row.output_name].setncattr('units', units)
        DatasetModeStore._validate(file_name, rows.output_name.unique())


def validate_plan(planner, frame, workspace):
    """Reject incomplete inputs and outputs shared by incompatible member/time jobs."""
    input_paths = {str(Path(path).resolve()) for path in frame.file_source.dropna().unique()}
    canonical = lambda path: str(path)[:-3] if str(path).endswith('.gz') else str(path)
    input_paths = {canonical(path) for path in input_paths}
    owners = {}
    for row in frame.itertuples(index=False):
        if row.file_source is None or not Path(row.file_source).is_file():
            raise FileNotFoundError(f'{row.time_mode}.{row.variable} at {row.time}: missing {row.file_source}')
        output = Path(row.file_destination).resolve()
        if output.is_relative_to(workspace):
            raise ValueError(f'Member output must be outside the cache workspace: {output}')
        key = canonical(output)
        if key in input_paths:
            raise ValueError(f'Member output would overwrite an upstream input: {output}')
        owner = (row.time, row.collection)
        if key in owners and owners[key] != owner:
            raise ValueError(f'Member output is shared by different times or collections: {output}')
        owners[key] = owner
    probabilistic = frame.loc[frame['mode'].isin(('probabilistic', 'probabilistic_ensemble'))]
    for output, rows in probabilistic.groupby('file_destination', sort=False):
        if rows.ensemble_member.nunique() > 1:
            raise ValueError(f'Probabilistic members need separate destination files: {output}')


def run(planner, workflow, workspace, logger, *, time_mode=None, reuse=True):
    """Prepare shared fields once, then copy and update complete member files."""
    workspace = Path(workspace).expanduser().resolve()
    frame = planner.file_plan
    if time_mode is not None:
        frame = frame.loc[frame.time_mode.eq(time_mode)].copy()
    if frame.empty:
        raise ValueError('No dataset steps selected.')
    validate_plan(planner, frame, workspace)
    for group in frame.time_mode.unique():
        selected_workflows(workflow, frame.loc[frame.time_mode.eq(group), 'variable'].unique(), group)
    terrain = planner.application['geo']['terrain']
    terrain_path = Path(terrain['path']) / terrain['file_name']
    geo_data = DataLocal(
        path=str(terrain_path.parent), file_name=terrain_path.name,
        file_type='grid_2d', file_format=terrain.get('format', 'ascii'), file_mode='local',
        file_variable='terrain', file_io='input', file_workflow='GEO',
        variable_template={'dims_geo': {'x': 'longitude', 'y': 'latitude'},
                           'vars_geo': {'x': 'longitude', 'y': 'latitude'}},
        time_signature=None, time_direction=None, logger=logger, message=False)
    store = DatasetModeStore(workspace / 'deterministic', reuse=reuse)
    shared = planner.file_plan_by_mode('deterministic', time_mode=time_mode)
    bases = {}
    computed, reused = 0, 0
    for (time_mode_key, stamp, collection), rows in shared.groupby(
            ['time_mode', 'time', 'collection'], sort=False):
        context = {
            'time': pd.Timestamp(stamp).isoformat(), 'collection': collection,
            'time_mode': rows.iloc[0]['time_mode'], 'workflow': workflow,
            'geography': file_identity(terrain_path),
            'variables': [{
                'variable': row.variable, 'output_name': row.output_name,
                'source': file_identity(row.file_source),
                'source_settings': planner.source_settings(row.variable, row.time_mode),
                'destination_settings': planner.destination_settings(row.variable, row.time_mode),
            } for row in rows.itertuples(index=False)],
        }
        path, was_reused = store.prepare(
            context, rows.output_name.unique(),
            lambda output, selected=rows: process_step(
                planner, workflow, geo_data, selected, output, workspace, logger))
        bases[(time_mode_key, stamp, collection)] = path
        reused += int(was_reused)
        computed += int(not was_reused)
        logger.info(f'Deterministic {stamp}: {"reused" if was_reused else "processed"} {list(rows.variable)}')

    published, summaries = {}, {}
    for member in planner.members or ('deterministic',):
        member_rows = frame.loc[frame.output_member.isna() | frame.output_member.eq(member)]
        execution = {}
        for (time_mode_key, stamp, collection, output), rows in member_rows.groupby(
                ['time_mode', 'time', 'collection', 'file_destination'], sort=False):
            probabilistic = rows.loc[rows['mode'].isin(('probabilistic', 'probabilistic_ensemble'))]
            base = bases.get((time_mode_key, stamp, collection))
            updates = []
            if not probabilistic.empty:
                # Stage all probabilistic fields for this member and expected
                # time together, then merge them into its deterministic copy.
                with tempfile.TemporaryDirectory(prefix='member-', dir=workspace) as folder:
                    delta = Path(folder) / 'probabilistic.nc'
                    process_step(planner, workflow, geo_data, probabilistic, delta, workspace, logger)
                    updates.append((delta, list(probabilistic.output_name.unique())))
                    destination_cfg = planner.destination_settings(rows.iloc[0]['variable'], time_mode_key)
                    published[output] = publish_member(
                        base, updates, output,
                        compress=destination_cfg.get('file_args', {}).get('file_compression', True))
            elif base is not None:
                destination_cfg = planner.destination_settings(rows.iloc[0]['variable'], time_mode_key)
                published[output] = publish_member(
                    base, updates, output,
                    compress=destination_cfg.get('file_args', {}).get('file_compression', True))
            else:
                raise ValueError(f'No deterministic base or probabilistic fields for {output} at {stamp}.')
            logger.info(f'Member {member} at {stamp}: copied deterministic fields and updated '
                        f'{list(probabilistic.variable)}')
            execution[pd.Timestamp(stamp).isoformat()] = {'status': 'completed'}
        if not member_rows.empty:
            summaries[member] = planner.summary(
                member=member, time_mode=time_mode, extra_args=execution, tag_args='execution')
    return {'deterministic_computed': computed, 'deterministic_reused': reused,
            'published_files': list(published.values()), 'summaries': summaries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-settings_file', required=True)
    parser.add_argument('-time')
    parser.add_argument('--workspace', help='Shared deterministic cache; defaults below application.tmp.path.')
    parser.add_argument('--no-reuse', action='store_true', help='Recompute deterministic cache entries.')
    parser.add_argument('--time-mode', choices=('all', 'observed', 'forecast'), default='all')
    args = parser.parse_args()
    application, workflow, tags = load_configuration(args.settings_file)
    log_cfg = application['log']
    LoggingManager.setup(
        logger_folder=log_cfg['path'], logger_file='mode_' + log_cfg['file_name'],
        handlers=['file', 'stream'], force_reconfigure=True)
    logger = LoggingManager(name='shybox_runner_dataset_modes', level=logging.INFO, set_as_current=True)
    planning = application['planning']
    planner = DatasetPlanner.from_config(
        info=application['info'], time=application['time'],
        data_src_obs=application['data_source']['observed'], data_src_frc=application['data_source']['forecast'],
        data_dst_obs=application['data_destination'], data_dst_frc=application['data_destination'],
        geo=application.get('geo'), time_run=args.time, time_tags=tags, logger=logger,
        options=planning['options'], files=planning.get('files'), summary=planning.get('summary'))
    if str(application['info'].get('model', 'hmc')).lower() != 'hmc':
        raise ValueError('This mode runner prepares HMC forcing only.')
    workspace = Path(args.workspace or str(Path(application['tmp']['path']) / 'dataset_modes')).expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    planner.update_time(pickle_history=str(workspace / 'planning.workspace'))
    result = run(planner, workflow, workspace, logger,
                 time_mode=None if args.time_mode == 'all' else args.time_mode, reuse=not args.no_reuse)
    logger.info(f'Mode execution complete: {result["deterministic_computed"]} shared steps processed, '
                f'{result["deterministic_reused"]} reused, {len(result["published_files"])} files published')
    return result


if __name__ == '__main__':
    main()
