"""Independent preparation/execution code for the parallel dataset variant.

Original workflow scripts are never imported or executed. Shared SHYBOX APIs
are used without modification. Planning happens in the parent; workers receive
configuration and planned paths, and construct their own handlers/orchestrators.
"""
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import traceback
from uuid import uuid4

from .executor import iter_task_results


TIME_KEYS = (
    'time_run', 'time_frequency', 'time_rounding', 'time_look_observed', 'time_look_forecast',
    'time_horizon_observed', 'time_horizon_forecast', 'time_priority_observed', 'time_priority_forecast',
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent,
                                         prefix=path.name + '.', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, indent=2, allow_nan=False, default=str)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def make_logger(folder=None, name='planner'):
    from shybox.logging_toolkit.logging_handler import LoggingManager
    options = {'handlers': ['stream'], 'force_reconfigure': True}
    if folder is not None:
        options.update(logger_folder=str(folder), logger_file=name + '.log', handlers=['file'])
    LoggingManager.setup(**options)
    return LoggingManager(name='parallel_datasets.' + name, set_as_current=True)


def load_settings(file_name, logger=None):
    from shybox.config_toolkit.config_handler import ConfigManager
    logger = logger or make_logger()
    config = ConfigManager.from_source(
        str(Path(file_name).resolve()), root_key='configuration', application_key=None,
        auto_validate=True, auto_fill_lut=True, flat_variables=True, flat_key_mode='value',
        convert_none_to_nan=False, apply_time_template_for_none=False, logger=logger,
    )
    application = config.fill_obj_from_lut(
        section=config.get_section(section='application', raise_if_missing=True),
        resolve_time_placeholders=False, time_keys=TIME_KEYS,
    )
    workflow = config.fill_obj_from_lut(
        section=config.get_section(section='workflow', raise_if_missing=True),
        resolve_time_placeholders=False,
    )
    from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
    tags = DatasetPlanner.create_time_tags(
        template=config.get_section(section='template', raise_if_missing=True),
        formats=config.get_section(section='format', raise_if_missing=True),
    )
    return application, workflow, tags


def destination_configs(section, prefix=''):
    if 'file_name' in section:
        yield prefix or 'forcing', section
    else:
        for name, child in section.items():
            yield from destination_configs(child, prefix + '_' + name if prefix else name)


def canonical_output(path):
    path = str(Path(path).resolve())
    return path[:-3] if path.endswith('.gz') else path


def validate_ownership(tasks, run_dir):
    """A complete output belongs to exactly one member/time, across all jobs."""
    owners = {}
    input_paths = set()
    for task in tasks:
        for step in task['steps']:
            for variable in step['variables']:
                input_paths.add(canonical_output(variable['source_path']))
    for task in tasks:
        for step in task['steps']:
            owner = (task['member'], step['time'])
            for variable in step['variables']:
                path = Path(variable['destination_path']).resolve()
                if not path.is_relative_to(Path(run_dir).resolve()):
                    raise ValueError(f'Output escapes the isolated run directory: {path}')
                key = canonical_output(path)
                if key in input_paths:
                    raise ValueError(f'An output would overwrite an input: {path}')
                if key in owners and owners[key] != owner:
                    raise ValueError(f'Output shared by different member/time jobs: {path}')
                owners[key] = owner


@dataclass
class PreparedRun:
    planner: object
    application: dict
    workflow: dict
    tasks: list
    run_dir: Path
    time_mode: str
    task_unit: str

    def report(self):
        members = list(dict.fromkeys(task['member'] for task in self.tasks))
        return {
            'schema_version': 1, 'run_dir': str(self.run_dir), 'time_mode': self.time_mode,
            'task_unit': self.task_unit, 'members': members, 'tasks': len(self.tasks),
            'member_time_steps': sum(len(task['steps']) for task in self.tasks),
            'output_files': sorted({variable['destination_path'] for task in self.tasks
                                    for step in task['steps'] for variable in step['variables']}),
            'jobs': [{'task_id': task['task_id'], 'member': task['member'],
                      'times': [step['time'] for step in task['steps']]} for task in self.tasks],
        }


def prepare_run(application, workflow, *, run_dir=None, time_run=None, time_tags=None,
                time_mode='forecast', task_unit='auto', time_chunk=1, planning_options=None):
    """Plan an isolated run without reading scientific arrays or launching workers.

    The supplied application/workflow are copied. Destination roots, temporary
    workspaces, logs, summaries and planning history all belong to the new run.
    A nonempty/existing run directory is never reused or overwritten.
    """
    from shybox.runner_toolkit.datasets.planning_handler import DatasetPlanner
    from pandas.tseries.frequencies import to_offset
    if task_unit not in ('auto', 'member', 'time'):
        raise ValueError('task_unit must be auto, member or time.')
    if time_mode not in ('observed', 'forecast', 'all'):
        raise ValueError('time_mode must be observed, forecast or all.')
    if isinstance(time_chunk, bool) or not isinstance(time_chunk, int) or time_chunk < 1:
        raise ValueError('time_chunk must be a positive integer.')
    application, workflow = deepcopy(application), deepcopy(workflow)
    if run_dir is None:
        label = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid4().hex[:8]
        run_dir = Path(__file__).parent / 'runs' / label
    run_dir = Path(run_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    logger = make_logger(run_dir / 'logs')
    probabilistic = any(cfg.get('mode') in ('probabilistic', 'probabilistic_ensemble')
                        for sources in application['data_source'].values() for cfg in sources.values())
    member_folder = 'run_{ensemble_member}' if probabilistic else 'deterministic'
    for name, config in destination_configs(application['data_destination']):
        name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        config['path'] = str(run_dir / 'forcing' / name / member_folder)
    application['log'] = {'path': str(run_dir / 'logs'), 'file_name': 'planner.log'}
    application['tmp'] = {'path': str(run_dir / 'tmp'), 'file_name': None}
    planner = DatasetPlanner.from_config(
        info=application['info'], time=application['time'],
        data_src_obs=application['data_source']['observed'],
        data_src_frc=application['data_source']['forecast'],
        data_dst_obs=application['data_destination'], data_dst_frc=application['data_destination'],
        time_run=time_run, geo=application.get('geo'), time_tags=time_tags, logger=logger,
    )
    options = {'n_step_to_check_obs': 3, 'n_step_to_check_frc': 1,
               'flag_adjust_obs_frc': True, 'rules_to_fill_obs': 'fill_using_frc', 'rules_to_fill_frc': None}
    options.update(application.get('planning', {}))
    options.update(planning_options or {})
    # Never read/write the sequential runner's workspace, even if its settings request history.
    options.update(flag_history=False, reset_history=True, pickle_history=str(run_dir / 'plans.workspace'))
    planner.update_time(**options)
    frame = planner.file_plan
    if time_mode != 'all':
        frame = frame.loc[frame.time_mode.eq(time_mode)]
    selected_mode = None if time_mode == 'all' else time_mode
    source_paths = planner.file_by_ensemble('source', time_mode=selected_mode)
    destination_paths = planner.file_by_ensemble('destination', time_mode=selected_mode)
    source_settings = {
        group: {variable: (raw, cfg) for cfg in sources.values()
                for raw, variable in cfg['variable'].items()}
        for group, sources in planner.application['data_source'].items()
    }
    destination_settings = planner.application['data_destination']
    grouped = bool(set(destination_settings) & {'observed', 'forecast'})
    task_unit = ('member' if planner.members else 'time') if task_unit == 'auto' else task_unit
    tasks = []
    for member, times in source_paths.items():
        steps = []
        for stamp, variables in times.items():
            rows = frame.loc[frame.time.eq(stamp)]
            group = rows.iloc[0].group
            collections = destination_settings[group] if grouped else destination_settings
            if 'file_name' in collections:
                collections = {'collection': collections}
            step = {'time': stamp, 'group': group, 'variables': []}
            for variable, source_path in variables.items():
                if source_path is None:
                    raise ValueError(f'{member}.{stamp}.{variable}: no source configured.')
                raw_name, source = source_settings[group][variable]
                row = rows.loc[rows.variable.eq(variable)].iloc[0]
                destination = collections[row.collection]
                # Support both existing workflow naming conventions without changing their JSON.
                suffix = 'OBS' if group == 'observed' else 'FRC'
                candidates = [f'{variable.upper()}_WORKFLOW_{suffix}', f'{variable.upper()}_WORKFLOW']
                workflow_name = next((name for name in candidates if name in workflow['process_list']), None)
                if workflow_name is None:
                    raise ValueError(f'{variable}: no matching process list in {candidates}.')
                step['variables'].append({
                    'name': variable, 'raw_name': raw_name, 'output_name': row.output_name,
                    'source_path': source_path, 'destination_path': destination_paths[member][stamp][variable],
                    'source': deepcopy(source), 'destination': deepcopy(destination), 'workflow': workflow_name,
                })
            steps.append(step)
        groups = [steps] if task_unit == 'member' else [steps[i:i + time_chunk] for i in range(0, len(steps), time_chunk)]
        for index, group_steps in enumerate(groups):
            if not group_steps:
                continue
            task_id = f'member_{member}_{index:05d}'
            tasks.append({'task_id': task_id, 'member': member, 'steps': group_steps,
                          'workflow': workflow, 'geo': application.get('geo'),
                          'frequency': to_offset(planner.time_plan.frequency).freqstr,
                          'work_dir': str(run_dir / 'tmp' / task_id),
                          'log_dir': str(run_dir / 'logs')})
    validate_ownership(tasks, run_dir)
    prepared = PreparedRun(planner, application, workflow, tasks, run_dir, selected_mode, task_unit)
    write_json(run_dir / 'plan.json', prepared.report())
    return prepared


def stage_source(path, work_dir, file_format):
    """Keep decompression/GRIB indexes away from shared source directories."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f'Planned source file is missing: {path}')
    compressed = path.name.endswith('.gz')
    if not compressed and file_format not in ('grib', 'grib2'):
        return path
    key = hashlib.sha256(str(path.resolve()).encode()).hexdigest()[:16]
    target = Path(work_dir) / 'sources' / key / (path.name[:-3] if compressed else path.name)
    if not target.is_file():
        target.parent.mkdir(parents=True, exist_ok=True)
        if compressed:
            with gzip.open(path, 'rb') as source, target.open('wb') as output:
                shutil.copyfileobj(source, output)
        else:
            shutil.copyfile(path, target)
    return target


def configure_step(task, step, logger):
    """Construct all variable workflows locally; one worker owns the whole output."""
    import pandas as pd
    from shybox.dataset_toolkit.dataset_handler_local import DataLocal
    from shybox.orchestrator_toolkit.orchestrator_handler_config import OrchestratorConfig
    from shybox.orchestrator_toolkit.lib_orchestrator_utils_discovery import load_process
    stamp = pd.Timestamp(step['time'])
    work_dir = Path(task['work_dir']) / stamp.strftime('%Y%m%d%H%M%S')
    work_dir.mkdir(parents=True, exist_ok=True)
    workflow = deepcopy(task['workflow'])
    workflow.setdefault('options', {}).update(tmp_dir=str(work_dir))
    active = {}
    source_handlers, destination_handlers = {}, {}
    geo = task.get('geo')
    terrain = None
    if geo:
        config = geo.get('terrain', geo)
        terrain = DataLocal(
            path=config['path'], file_name=config['file_name'],
            file_type='grid_2d', file_format=config.get('format', 'ascii'), file_mode='local',
            file_variable='terrain', file_workflow='GEO', file_io='input',
            variable_template={'dims_geo': {'x': 'longitude', 'y': 'latitude'},
                               'vars_geo': {'x': 'longitude', 'y': 'latitude'}},
            time_signature=None, time_direction=None, logger=logger, message=False,
        )
    for variable in step['variables']:
        name, source, destination = variable['name'], variable['source'], variable['destination']
        workflow_name = variable['workflow']
        active[workflow_name] = deepcopy(workflow['process_list'][workflow_name])
        for process in active[workflow_name]:
            load_process(process['function'], search_locations=workflow.get('process_locations'))
        source_path = stage_source(variable['source_path'], task['work_dir'], source['format'])
        output_path = Path(variable['destination_path'])
        if output_path.name.endswith('.gz'):
            output_path = Path(str(output_path)[:-3])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_tag = f"{task['member']}:{name}_{stamp:%Y%m%d%H%M}_src"
        destination_tag = f"{task['member']}:{name}_{stamp:%Y%m%d%H%M}_dst"
        input_template = deepcopy(source.get('variable_template', {
            'dims_geo': {source['x']: 'longitude', source['y']: 'latitude', source['time']: 'time'},
        }))
        input_template['vars_data'] = {variable['raw_name']: name}
        output_template = deepcopy(destination.get('variable_template', {
            'dims_geo': {'longitude': destination.get('x', 'west_east'),
                         'latitude': destination.get('y', 'south_north'), 'time': destination.get('time', 'time')},
            'coords_geo': {'Longitude': 'longitude', 'Latitude': 'latitude'},
        }))
        output_template['vars_data'] = {name: variable['output_name']}
        output_template['units'] = {name: destination.get('units', {}).get(name, 'NA')}
        source_handlers[source_tag] = DataLocal(
            path=str(source_path.parent), file_name=source_path.name,
            file_type=source.get('file_type', 'grid_' + source['dims']), file_format=source['format'],
            file_mode='local', file_variable=name, file_io='input', file_workflow=workflow_name,
            variable_template=input_template, file_args=deepcopy(source.get('file_args', {})),
            time_signature='current', time_reference=stamp, time_period=1,
            time_freq=task['frequency'], time_direction='forward', logger=logger, message=False, tag=source_tag,
        )
        model_type = {'hmc': 'forcing_hmc', 'hmc_forcing': 'forcing_hmc',
                      's3m': 'forcing_s3m', 's3m_forcing': 'forcing_s3m'}.get(destination['type'], destination['type'])
        output_args = deepcopy(destination.get('file_args', {}))
        output_args.setdefault('compress', destination.get('compression_flag', True))
        output_args.setdefault('file_compression', destination.get(
            'as_compress', variable['destination_path'].endswith('.gz')))
        destination_handlers[destination_tag] = DataLocal(
            path=str(output_path.parent), file_name=output_path.name,
            file_type=destination.get('file_type', model_type), file_format=destination['format'],
            file_mode='local', file_variable=name, file_io='output', file_workflow=workflow_name,
            variable_template=output_template, file_args=output_args,
            time_signature='step', time_reference=stamp, time_period=1,
            time_freq=task['frequency'], time_direction='forward', logger=logger, message=False, tag=destination_tag,
        )
    workflow['process_list'] = active
    workflow['priority'] = [name for name in workflow.get('priority', []) if name in active]
    return OrchestratorConfig.configure(
        data_package_in=source_handlers, data_package_out=destination_handlers, data_ref=terrain,
        configuration=workflow, priority=workflow['priority'], logger=logger,
        tag_orchestrator=f"parallel_{task['task_id']}_{stamp:%Y%m%d%H%M}", tag_logger='ParallelDataset',
    )


def execute_task(task):
    """Worker entry point: build local objects, run sequential steps, return traces."""
    logger = make_logger(task['log_dir'], task['task_id'])
    result = {'task_id': task['task_id'], 'member': task['member'], 'pid': os.getpid(),
              'status': 'completed', 'execution': {}}
    for step in task['steps']:
        started_at, timer = utc_now(), time.monotonic()
        report = {'status': 'failed', 'started_at': started_at}
        try:
            orchestrator = configure_step(task, step, logger)
            orchestrator.run(time=step['time'], group='by:step')
            report['status'] = 'completed'
        except Exception as exc:
            report['error'] = f'{type(exc).__name__}: {exc}'
            result.update(status='failed', error=report['error'], traceback=traceback.format_exc())
        report.update(finished_at=utc_now(), duration_seconds=round(time.monotonic() - timer, 3))
        report['source'] = {var['name']: var['source_path'] for var in step['variables']}
        report['destination'] = {var['name']: var['destination_path'] for var in step['variables']}
        result['execution'][step['time']] = report
        if result['status'] == 'failed':
            break
    return result


def run_parallel_datasets(prepared, *, max_workers=2, native_threads=1, worker=execute_task):
    """Run the prepared jobs and summarize a member only after all its jobs finish."""
    if not prepared.tasks:
        raise ValueError('The selected plan has no member/time tasks.')
    remaining = Counter(task['member'] for task in prepared.tasks)
    execution, summaries, task_results = {}, {}, {}
    for task, result in iter_task_results(prepared.tasks, worker,
                                          max_workers=max_workers, native_threads=native_threads):
        member = task['member']
        task_results[task['task_id']] = result
        write_json(prepared.run_dir / 'reports' / (task['task_id'] + '.json'), result)
        records = execution.setdefault(member, {})
        records.update(result.get('execution', {}))
        if result['status'] != 'completed' and not result.get('execution'):
            records['task:' + task['task_id']] = {'status': 'failed', 'error': result.get('error', 'worker failed')}
        remaining[member] -= 1
        if remaining[member] == 0:
            summaries[member] = prepared.planner.summary(
                str(prepared.run_dir / 'summaries' / 'member.{member}.json'), member=member,
                time_mode=prepared.time_mode, extra_args=records, tag_args='execution',
            )
    merged = prepared.planner.summary_execution(summaries, prepared.run_dir / 'summaries' / 'all_members.json')
    ready = all(record['ready'] for record in merged.values())
    completed = all(result['status'] == 'completed' for result in task_results.values())
    report = {'status': 'completed' if ready and completed else 'failed', 'run_dir': str(prepared.run_dir),
              'ready': ready, 'tasks': task_results, 'summary_files': {
                  member: str(prepared.run_dir / 'summaries' / f'member.{member}.json') for member in summaries},
              'merged_summary': str(prepared.run_dir / 'summaries' / 'all_members.json')}
    write_json(prepared.run_dir / 'execution.json', report)
    return report
