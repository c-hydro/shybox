"""Compose observed/forecast grids into validated HMC/S3M forcing files.

Only one timestamp and the last valid field per variable are held in memory.
Times are naive UTC; precipitation must already represent model-step amounts.
"""
from contextlib import contextmanager, ExitStack, nullcontext
from copy import deepcopy
from dataclasses import replace
import gzip
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
from inspect import signature

import numpy as np
import pandas as pd
import xarray as xr

from shybox.logging_toolkit.logging_handler import LoggingManager

from shybox.runner_toolkit.datasets.lib_utils_datasets import TimePeriod as ForcingPlan, create_time_period
from shybox.runner_toolkit.datasets.datasets_mode import DatasetMode
from shybox.runner_toolkit.datasets.lib_utils_datasets import split_3d_to_2d, resample_data, copy_as_is
from shybox.runner_toolkit.datasets.lib_utils_datasets_geo import read_geographical_info, check_geographical_info


VARIABLES_MANDATORY = {'rain', 'air_t', 'inc_rad', 'rel_hum', 'wind_speed'}
TIME_FIELDS_MANDATORY = {
    'time_run', 'time_rounding', 'time_frequency',
    'hour_reference_observed', 'hour_reference_forecast',
    'time_look_observed', 'time_horizon_observed',
    'time_look_forecast', 'time_horizon_forecast',
    'priority_observed', 'priority_forecast',
}
SOURCE_FIELDS_MANDATORY = {
    'path', 'file_name', 'x', 'y', 'time', 'nodata', 'format', 'variable',
    'operations', 'mode', 'ensemble', 'required', 'tolerance_steps', 'missing_policy', 'as_is',
}
DESTINATION_FIELDS_MANDATORY = {
    'path', 'file_name', 'format', 'type',
}


DEFAULT_TIME_TAGS = {
    'path_time_run': {'reference': 'time_run', 'format': '%Y%m%d%H%M'},
    'path_time_destination': {'reference': 'time', 'format': '%Y/%m/%d'},
    'file_time_destination': {'reference': 'time', 'format': '%Y%m%d%H%M'},
    **{name: {'reference': 'source_time', 'format': template}
       for name, template in (
           ('path_time_observed', '%Y/%m/%d'), ('file_time_observed', '%Y%m%d%H%M'),
           ('path_time_forecast_nwp', '%Y/%m/%d'), ('file_time_forecast_nwp', '%Y%m%d%H%M'),
           ('path_time_forecast_rfarm', '%Y/%m'), ('file_time_forecast_rfarm', '%Y%m%d%H%M'))},
}

# define class DatasetManager
class DatasetManager:

    def __init__(self, info, time, data_src_obs, data_src_frc,
                 data_dst_obs, data_dst_frc, time_run=None,
                 *,
                 logger: LoggingManager | None = None,
                 geo=None, time_tags=None, options=None):

        # Set up logger for this DatasetManager instance
        self.log = LoggingManager.get_logger(logger=logger, name="DatasetManager", set_as_current=False,)

        # iterate over needed object(s)
        for name, value in (('info', info), ('time', time),
                            ('data_src_obs', data_src_obs), ('data_src_frc', data_src_frc),
                            ('data_dst_obs', data_dst_obs), ('data_dst_frc', data_dst_frc)):
            # exit with errors (if one section is omitted)
            if not isinstance(value, dict):
                raise TypeError(f'DatasetManager {name} must be a dictionary.')

        # check missing time fields
        missing_fields_time = TIME_FIELDS_MANDATORY - time.keys()
        if missing_fields_time:
            raise ValueError(f'Missing dataset time fields: {sorted(missing_fields_time)}')

        # iterate over source obs/frc object(s)
        for group, data in (('observed', data_src_obs), ('forecast', data_src_frc)):
            for name, obj in data.items():
                if not isinstance(obj, dict):
                    raise TypeError(f'{group}.{name} must be a dictionary.')
                # check missing source fields
                missing_fields_src = SOURCE_FIELDS_MANDATORY - obj.keys()
                if missing_fields_src:
                    raise ValueError(f'{group}.{name}: missing source fields: {sorted(missing_fields_src)}')

        # iterate over destination obs/frc object(s)
        for group, data in (('observed', data_dst_obs), ('forecast', data_dst_frc)):
            for name, obj in data.items():
                if not isinstance(obj, dict):
                    raise TypeError(f'{group}.{name} must be a dictionary.')
                # check missing source fields
                missing_fields_dst = DESTINATION_FIELDS_MANDATORY - obj.keys()
                if missing_fields_dst:
                    raise ValueError(f'{group}.{name}: missing source fields: {sorted(missing_fields_dst)}')

        # define the application dataset
        application_datasets = {
            'info': info, 'time': time,
            'data_source': {'observed': data_src_obs, 'forecast': data_src_frc},
            'data_destination': {'observed': data_dst_obs, 'forecast': data_dst_frc}}

        # add if available the geo object
        if geo is not None:
            application_datasets['geo'] = geo if 'terrain' in geo else {'terrain': geo}

        application_variables = self._normalize_variables(application=application_datasets)

        self.time_run = time_run or time.get('time_run')
        self.time_plan = None
        self.mode = DatasetMode()
        self.members = ()


    @property
    def settings(self):
        return deepcopy(self._settings)

    @property
    def time_period(self):
        """Ordered validity times shared by the time table and file plan."""
        if self.time_plan is None:
            self.configure_time()
        return self.time_plan.time_period.copy()

    @property
    def time_table(self):
        if self.time_plan is None:
            self.configure_time()
        return self.time_plan.table.copy()

    @property
    def file_plan(self):
        if self._file_plan is None:
            self.plan_files()
        return self._file_plan.copy()

    @classmethod
    def from_application(cls, application, *, time_tags=None, options=None, time_run=None):

        return cls(application['info'], application['time'],
                   application['data_source']['observed'], application['data_source']['forecast'],
                   application['data_destination']['observed'], application['data_destination']['forecast'],
                   time_run=time_run, geo=application.get('geo'),
                   time_tags=time_tags, options=options)

    @staticmethod
    def create_time_tags(template, formats, *, references=None):
        """Build deferred date tags from SHYBOX template/format dictionaries.

        Select path_time_*/file_time_* entries declared as "time". Run tags
        use time_run; destination and *_valid tags use validity time; other
        tags use source_time. Explicit references can select custom tag names
        and override these defaults. Ordinary time settings are left alone.
        """
        for name, value in (('template', template), ('formats', formats)):
            if not isinstance(value, dict):
                raise TypeError(f'{name} must be a dictionary.')
        if references is None:
            references = {}
        if not isinstance(references, dict):
            raise TypeError('references must be a dictionary.')
        valid_references = {'time_run', 'forecast_run', 'time', 'source_time'}
        for name, reference in references.items():
            if not isinstance(reference, str) or reference not in valid_references:
                raise ValueError(f'{name}: unsupported time reference {reference!r}.')
            if formats.get(name) != 'time':
                raise ValueError(f'{name}: custom time tags must have format "time".')
        tags = {}
        for name, kind in formats.items():
            if kind != 'time' or not (name.startswith(('path_time_', 'file_time_')) or name in references):
                continue
            date_format = template.get(name)
            if not isinstance(date_format, str) or not date_format.strip():
                raise ValueError(f'{name}: a nonempty time template is required.')
            if name in ('path_time_run', 'file_time_run'):
                reference = 'time_run'
            elif name in ('path_time_destination', 'file_time_destination') or name.endswith('_valid'):
                reference = 'time'
            else:
                reference = 'source_time'
            tags[name] = {'reference': references.get(name, reference), 'format': date_format}
        return tags

    def _normalize_variables(self, application):

        application_src_data = application['data_source']
        application_dst_data = application['data_destination']

        # check variable source
        variables_src, path_src = {}, {}
        for key, obj in application_src_data.items():
            for variable, fields in obj.items():
                if not 'variable' in fields:
                    self.log.error(f'{key} has no variable fields.')
                    raise RuntimeError(f'{variable} is mandatory in declaration.')

                variable_map = fields['variable']
                for key, name in variable_map.items():
                    variables_src[key] = name

                if not 'file_name' in fields and 'path' in fields:
                    self.log.error(f'{key} has no file_name or path fields.')
                    raise RuntimeError(f'{variable} is mandatory in declaration.')

                for key, name in variable_map.items():
                    path_src[key] = os.path.join(fields['path'], fields['file_name'])

        # check variable out
        variables_dst, path_dst = {}, {}
        for key, obj in application_dst_data.items():
            for variable, fields in obj.items():
                if not 'variable' in fields:
                    self.log.error(f'{key} has no variable fields.')
                    raise RuntimeError(f'{variable} is mandatory in declaration.')

                variable_map = fields['variable']
                for key, name in variable_map.items():
                    variables_dst[key] = name

                if not 'file_name' in fields and 'path' in fields:
                    self.log.error(f'{key} has no file_name or path fields.')
                    raise RuntimeError(f'{variable} is mandatory in declaration.')

                for key, name in variable_map.items():
                    path_dst[key] = os.path.join(fields['path'], fields['file_name'])

        # check that each source variable maps to a destination key.
        missing_variable = set(variables_src.values()) - variables_dst.keys()
        if missing_variable:
            message = f'Source variables missing from destination: {sorted(missing_variable)}'
            self.log.error(message)
            raise RuntimeError(message)

        # map original source names directly to final destination names.
        variables_mapping = {
            source_name: variables_dst[canonical_name]
            for source_name, canonical_name in variables_src.items()
        }

        variables_collections = {
            'source': variables_src,
            'destination': variables_dst,
            'mapping': variables_mapping
        }

        return variables_collections

    def configure_destination(self, *, file_name_observed=None, file_name_forecast=None):
        """Expand the shared destination with optional per-group filenames.

        Omitted filenames use the shared file_name. Calling without overrides
        restores the common template. Output variables, units and run/member
        folders stay shared. A successful change invalidates cached file plans
        and output_path, preserving the configured time and source modes.
        """
        destination = deepcopy(self._settings['destination'])
        names = {
            'observed': destination['file_name'] if file_name_observed is None else file_name_observed,
            'forecast': destination['file_name'] if file_name_forecast is None else file_name_forecast,
        }
        for group, name in names.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f'{group} destination file_name must be a nonempty string.')
        destination['file_names'] = names
        self._settings['destination'] = destination
        self._file_plan = None
        self.output_path = None
        return deepcopy(destination)

    def configure_time(self, time_run=None, **options):

        selected_run = self.time_run if time_run is None else time_run
        selected_options = {**self._time_options, **options}
        plan = time_configure(self._settings, selected_run, **selected_options)
        self.time_plan = plan
        self._base_time_plan = plan
        self.time_run = selected_run or plan.time_run
        self._time_options = selected_options
        self._file_plan = None
        self.output_path = None
        self.log.info('Configured time period: run=%s, frequency=%s, observed=%d, forecast=%d, total=%d steps',
                 plan.time_run, plan.frequency, len(plan.observed), len(plan.forecast), len(plan.time_period))
        if LOG.isEnabledFor(logging.INFO):
            LOG.info('Configured time steps:\n%s', plan.table.to_string(
                columns=['step', 'time_step', 'time_mode', 'time_reference', 'file_id', 'time_checked'],
                formatters={'time_step': lambda value: f'{value:+d}' if value else '0'},
                max_rows=None))
        return plan

    def configure_mode(self):

        mode, members = plan_datasets_probabilistic(self._settings)
        self.mode = DatasetMode(mode)
        self.members = tuple(members)
        self.source_modes = {group: {key: DatasetMode.from_source(source) for key, source in sources.items()}
                             for group, sources in self._settings['sources'].items()}
        self._mode_configured = True
        self._file_plan = None
        self.output_path = None
        return self.mode

    def update_time(self, n_steps_observed=None, n_steps_forecast=None):
        """Check the window [-n_steps_observed, +n_steps_forecast] around zero.

        Replace an observed step with forecast only when all required forecast
        files exist for every output member at that same validity time. Optional
        variables do not trigger replacement. Missing both sources stays missing.
        Repeated calls start from the configured period, allowing observations
        that arrive later to restore the original source selection. Only file
        presence is checked; composition still validates timestamps and fields.
        None disables that side. Both None performs no availability checks.
        """
        for name, value in (('n_steps_observed', n_steps_observed), ('n_steps_forecast', n_steps_forecast)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f'{name} must be a nonnegative integer or None.')
        if self.time_plan is None:
            self.configure_time()
        base = self._base_time_plan or self.time_plan
        self.time_plan = base
        steps = base.table.time_step
        check_window = pd.Series(False, index=base.time_period)
        if n_steps_observed is not None:
            check_window |= (steps >= -n_steps_observed) & (steps <= 0)
        if n_steps_forecast is not None:
            check_window |= (steps >= 0) & (steps <= n_steps_forecast)
        checked_times = base.table.index[check_window]
        LOG.info('Update time: observed window=%s, forecast window=%s; checking %d of %d steps',
                 'disabled' if n_steps_observed is None else f'[-{n_steps_observed}, 0]',
                 'disabled' if n_steps_forecast is None else f'[0, +{n_steps_forecast}]',
                 len(checked_times), len(base.time_period))
        if n_steps_observed is None and n_steps_forecast is None:
            LOG.info('Time availability checks disabled: both step limits are None')
        files = self.plan_files(check_exists=True, check_times=checked_times)
        # A separate candidate table supplies forecast-cycle references even at
        # timestamps originally assigned to observed data.
        candidate = ForcingPlan(base.time_run, base.forecast_run, base.observed[:0], base.time_period,
                                observed_run=base.observed_run, frequency=base.frequency)
        required = {
            group: MANDATORY | {key for key, cfg in self._settings['sources'][group].items()
                               if cfg.get('required', False)}
            for group in ('observed', 'forecast')}
        replacement, existence = [], {}
        for stamp in base.observed.intersection(checked_times):
            observed = files[(files.time == stamp) & files.variable.isin(required['observed'])]
            if observed.source_exists.all():
                continue
            complete = True
            for member in self.members or (None,):
                for key in required['observed'] | required['forecast']:
                    source = self._settings['sources']['forecast'].get(key)
                    if not source:
                        complete = False
                        continue
                    path = _path(source, candidate, stamp, self._settings.get('time_tags', {}), member).resolve()
                    if path not in existence:
                        existence[path] = path.is_file()
                    complete = complete and existence[path]
            if complete:
                replacement.append(stamp)
                LOG.info('Time step %+d at %s: observed files missing; using forecast files',
                         steps.loc[stamp], stamp)
            else:
                LOG.info('Time step %+d at %s: observed files missing and forecast replacement incomplete',
                         steps.loc[stamp], stamp)
        table = base.table.copy()
        if replacement:
            table.loc[replacement, ['time_mode', 'time_reference_step', 'time_reference_3d', 'time_reference']] = (
                candidate.table.loc[replacement, ['time_mode', 'time_reference_step', 'time_reference_3d', 'time_reference']])
            # Honor step-based forecasts when no source uses cube references.
            forecast_mode = '3d' if any(
                (source.get('reference_mode') or ('3d' if (source.get('operations') or {}).get('split') else 'step')) == '3d'
                for source in self._settings['sources']['forecast'].values()) else 'step'
            forecast_mode = self._time_options.get(
                'reference_mode_forecast', self._settings['time'].get('reference_mode_forecast', forecast_mode))
            table.loc[replacement, 'time_reference'] = table.loc[replacement, 'time_reference_' + forecast_mode]
        self.time_plan = replace(base, observed=base.observed.difference(replacement),
                                 forecast=base.forecast.union(pd.DatetimeIndex(replacement)).sort_values(), table=table)
        refreshed = self.plan_files(check_exists=True, check_times=checked_times)
        availability = {}
        for stamp in checked_times:
            group = 'observed' if self.time_plan.table.loc[stamp, 'time_mode'] == 'obs' else 'forecast'
            rows = refreshed[(refreshed.time == stamp) & refreshed.variable.isin(required[group])]
            availability[stamp] = bool(rows.source_exists.all())
            missing = sorted(rows.loc[~rows.source_exists.astype(bool), 'variable'].unique())
            LOG.info('Time step %+d at %s: source=%s, required files available=%d/%d, missing variables=%s',
                     steps.loc[stamp], stamp, group, int(rows.source_exists.sum()), len(rows),
                     ', '.join(missing) if missing else 'none')
        self.time_plan.table['time_checked'] = self.time_plan.table.index.isin(checked_times)
        self.time_plan.table['time_available'] = pd.Series(availability, dtype=object).reindex(self.time_period)
        self.output_path = None
        LOG.info('Time update complete: checked=%d, available=%d, unavailable=%d, '
                 'forecast replacements=%d, unchecked=%d',
                 len(availability), sum(availability.values()),
                 len(availability) - sum(availability.values()), len(replacement),
                 len(base.time_period) - len(availability))
        if LOG.isEnabledFor(logging.INFO):
            LOG.info('Updated time steps:\n%s', self.time_plan.table.to_string(
                columns=['step', 'time_step', 'time_mode', 'time_reference', 'file_id',
                         'time_checked', 'time_available'],
                formatters={'time_step': lambda value: f'{value:+d}' if value else '0'},
                max_rows=None))
        return self.time_plan

    def configure(self, time_run=None, **time_options):
        self.configure_time(time_run, **time_options)
        self.configure_mode()
        return self

    def plan_files(self, check_exists=False, *, check_times=None):
        """Plan each step/member/variable, optionally checking source and output files.

        Steps are numbered from one across the complete observed/forecast period.
        Existence means a regular file is present, not that its contents are valid.
        Unchecked flags are null; an unconfigured source is false when checked.
        Call again with check_exists=True to refresh availability after file changes.
        check_times restricts existence checks to those validity timestamps.
        """
        if self.time_plan is None:
            self.configure_time()
        if not self._mode_configured:
            self.configure_mode()

        settings, plan = self._settings, self.time_plan
        check_times = None if check_times is None else set(check_times)
        variables, destination, tags = settings['variables'], settings['destination'], settings.get('time_tags', {})
        if not MANDATORY <= variables.keys():
            raise ValueError(f'Missing mandatory variable definitions: {sorted(MANDATORY - variables.keys())}')
        output_names = [value['output_name'] for value in variables.values()]
        if len(output_names) != len(set(output_names)) or set(output_names) & {'longitude', 'latitude', 'time', 'crs', 'terrain'}:
            raise ValueError('Output variable names must be unique and not reserved.')
        root = _path({'folder_name': destination['folder_name']}, plan, time_tags=tags).resolve()
        rows, owners, exists = [], {}, {}
        steps = {stamp: index for index, stamp in enumerate(plan.time_period, start=1)}
        for member in self.members or (None,):
            member_root = root
            if member is not None:
                template = destination.get('ensemble_folder', 'run_{ensemble_member}')
                if '{ensemble_member}' not in template:
                    raise ValueError('ensemble_folder must contain {ensemble_member}.')
                folder = _path({'file_name': template}, plan, time_tags=tags, ensemble_member=member)
                if folder.is_absolute() or '..' in folder.parts or folder == Path('.') or folder.name == 'manifest.json':
                    raise ValueError('ensemble_folder must stay inside the forcing run folder.')
                member_root = root / folder
            for group, times in (('observed', plan.observed), ('forecast', plan.forecast)):
                for stamp in times:
                    check_step = check_exists and (check_times is None or stamp in check_times)
                    relative = _path({'file_name': _destination_filename(destination, group)}, plan, stamp, time_tags=tags)
                    if relative.is_absolute() or '..' in relative.parts or relative.name == 'manifest.json':
                        raise ValueError('Output path must be unique and inside forcing folder.')
                    if not str(relative).endswith(('.nc', '.nc.gz')):
                        raise ValueError('Output filenames must end with .nc or .nc.gz.')
                    output = (member_root / relative).resolve()
                    owner = (member, stamp)
                    if output in owners and owners[output] != owner:
                        raise ValueError(f'Output paths collide for different times/members: {output}')
                    owners[output] = owner
                    if check_step and output not in exists:
                        exists[output] = output.is_file()
                    for key, variable in variables.items():
                        source = settings['sources'][group].get(key)
                        source_mode = DatasetMode.from_source(source or {})
                        if source_mode.is_probabilistic:
                            source_mode = source_mode.for_member(member)
                        path, reference = None, None
                        if source:
                            path = _path(source, plan, stamp, time_tags=tags, ensemble_member=member).resolve()
                            reference_mode = source.get('reference_mode') or ('3d' if (source.get('operations') or {}).get('split') else 'step')
                            reference = plan.reference_time(stamp, reference_mode)
                            if check_step and path not in exists:
                                exists[path] = path.is_file()
                        rows.append({'step': steps[stamp], 'time_step': int(plan.table.loc[stamp, 'time_step']),
                                     'time': stamp, 'time_mode': plan.table.loc[stamp, 'time_mode'],
                                     'time_reference': reference, 'ensemble_member': member or 'deterministic',
                                     **source_mode.as_dict(), 'variable': key, 'output_name': variable['output_name'],
                                     'source_name': source.get('source_name', key) if source else None,
                                     'file_source': str(path) if path else None, 'file_destination': str(output),
                                     'source_exists': (exists.get(path, False) if check_step else None),
                                     'destination_exists': (exists[output] if check_step else None)})
        columns = ['step', 'time_step', 'time', 'time_mode', 'time_reference', 'ensemble_member', 'mode', 'member', 'variable',
                   'output_name', 'source_name', 'file_source', 'file_destination', 'source_exists', 'destination_exists']
        self._file_plan = pd.DataFrame(rows, columns=columns).sort_values(
            ['step', 'ensemble_member'], kind='stable').reset_index(drop=True)
        for kind in ('source', 'destination'):
            self._file_plan[kind + '_file_id'] = pd.factorize(self._file_plan['file_' + kind], sort=False)[0] + 1
            counts = self._file_plan.groupby('time')['file_' + kind].nunique()
            plan.table[kind + '_file_count'] = counts.reindex(plan.time_period, fill_value=0).astype(int)
        return self._file_plan.copy()

    def as_dict(self, include_files=False):

        if self._file_plan is None:
            self.plan_files()
        result = {**self.time_plan.as_dict(), 'run_mode': self.mode.run_mode,
                  'ensemble_members': list(self.members),
                  'source_files': self._file_plan.file_source.nunique(),
                  'destination_files': self._file_plan.file_destination.nunique()}
        for kind in ('source', 'destination'):
            files = self._file_plan.dropna(subset=['file_' + kind, kind + '_exists']).drop_duplicates('file_' + kind)
            checked = files[kind + '_exists'].dropna()
            result[kind + '_files_available'] = int(checked.sum()) if len(checked) else None
            result[kind + '_files_missing'] = int((~checked.astype(bool)).sum()) if len(checked) else None
        if include_files:
            files = self._file_plan.copy()
            for column in ('time', 'time_reference'):
                files[column] = files[column].map(lambda value: str(value) if pd.notna(value) else None)
            result['file_plan'] = files.to_dict(orient='records')
        return result

    def run(self):
        if self._settings.get('geo') is None:
            raise ValueError('A reference geo grid is required to execute datasets; pass geo to DatasetManager.')
        if self._file_plan is None:
            self.plan_files()
        self.output_path = compose_datasets(self._settings, self.time_run, plan=self.time_plan)
        return self.output_path


def time_configure(settings=None, time_run=None, **options):
    """Configure periods/table from normalized settings, a time dictionary or LUT.

    Explicit keyword options override settings. Source step/3D modes select the
    table reference column; the two reference alternatives remain available.
    """
    settings = settings or {}
    cfg = dict(settings.get('time', settings))
    if 'observed' in cfg and 'forecast' in cfg and 'time_look_observed' not in cfg:
        return plan_datasets_deterministic(settings if 'time' in settings else {'time': cfg}, time_run or cfg.get('time_run'))
    aliases = {'time_priority_observed': 'priority_observed', 'time_priority_forecast': 'priority_forecast'}
    for source, target in aliases.items():
        if source in cfg:
            cfg.setdefault(target, cfg[source])
    cfg.update(options)
    for source, target in aliases.items():
        if source in options:
            cfg[target] = options[source]
    accepted = set(signature(create_time_period).parameters)
    unknown = set(options) - accepted - set(aliases)
    if unknown:
        raise ValueError(f'Unknown time options: {sorted(unknown)}')
    arguments = {key: value for key, value in cfg.items() if key in accepted}
    if time_run is not None:
        arguments['time_run'] = time_run
    if not arguments.get('time_run'):
        raise ValueError('time_configure requires time_run in settings or arguments.')
    for group in ('observed', 'forecast'):
        modes = {source.get('reference_mode') or ('3d' if (source.get('operations') or {}).get('split') else 'step')
                 for source in settings.get('sources', {}).get(group, {}).values()}
        if modes:
            arguments.setdefault('reference_mode_' + group, '3d' if '3d' in modes else 'step')
    return create_time_period(**arguments)


def plan_datasets_deterministic(settings, time_run):
    """Observed includes the cutoff; forecast starts one forcing step later."""
    cfg = settings['time']
    if 'time_look_observed' in cfg:
        return time_configure(settings, time_run)
    run = pd.Timestamp(time_run)
    if pd.isna(run) or run.tzinfo is not None:
        raise ValueError('time_run must be a valid naive UTC timestamp.')
    run = run.floor(cfg.get('rounding', '30min'))
    step = pd.Timedelta(cfg.get('frequency', '1h'))
    lookback = pd.Timedelta(cfg['observed']['lookback'])
    steps = cfg['forecast']['n_steps_model']
    if step <= pd.Timedelta(0) or lookback < pd.Timedelta(0):
        raise ValueError('frequency must be positive and lookback nonnegative.')
    if isinstance(steps, bool) or not isinstance(steps, int) or steps < 0:
        raise ValueError('n_steps_model must be a nonnegative integer.')
    cutoff = run.floor(step)
    start = run - lookback
    if cfg['observed'].get('floor_start'):
        start = start.floor(cfg['observed']['floor_start'])
    observed = pd.date_range(start.ceil(step), cutoff, freq=step)
    forecast_cfg = cfg['forecast']
    issue = pd.Timestamp(forecast_cfg.get('time_run') or run.floor(forecast_cfg.get('rounding', '1h')))
    if pd.isna(issue) or issue.tzinfo is not None or issue > run:
        raise ValueError('Forecast issue time must be naive UTC and cannot be in the future.')
    reference = forecast_cfg.get('horizon_reference', 'midnight')
    if reference not in ('midnight', 'forecast_run'):
        raise ValueError('horizon_reference must be midnight or forecast_run.')
    anchor = issue.normalize() if reference == 'midnight' else issue
    end = anchor + steps * step
    if end != end.floor(step):
        raise ValueError('Forecast horizon must align with the forcing frequency.')
    forecast = pd.date_range(cutoff + step, end, freq=step)
    return ForcingPlan(run, issue, observed, forecast, frequency=step)


def _destination_filename(destination, group):
    return destination.get('file_names', {}).get(group, destination['file_name'])


def _path(cfg, plan, time=None, time_tags=None, ensemble_member=None):
    tags = {'time_run': plan.time_run, 'forecast_run': plan.forecast_run, 'time': time,
            'ensemble_member': ensemble_member}
    reference_mode = cfg.get('reference_mode') or ('3d' if (cfg.get('operations') or {}).get('split') else 'step')
    tags['source_time'] = plan.reference_time(time, reference_mode) if time is not None else None
    tags['time_mode'] = plan.table.loc[pd.Timestamp(time), 'time_mode'] if time is not None else None
    for name, declaration in (time_tags or {}).items():
        reference = tags.get(declaration['reference'])
        if reference is not None:
            tags[name] = reference.strftime(declaration['format'])
    raw = os.path.join(cfg.get('folder_name', ''), cfg.get('file_name', ''))
    raw = os.path.expandvars(os.path.expanduser(raw))
    if '$' in raw:
        raise ValueError(f'Unresolved environment variable in path: {raw}')
    if '{ensemble_member}' in raw and ensemble_member is None:
        raise ValueError('An ensemble_member path placeholder requires a probabilistic variable/member.')
    return Path(raw.format(**tags))


@contextmanager
def _uncompressed_file(path):
    # Unzip on disk, not in RAM, for every supported source format.
    with tempfile.TemporaryDirectory(prefix='shybox-forcing-read-') as temporary:
        source = path
        if path.suffix == '.gz':
            source = Path(temporary) / path.stem
            with gzip.open(path, 'rb') as reader, source.open('wb') as writer:
                shutil.copyfileobj(reader, writer)
        yield source


@contextmanager
def _open_dataset(path):
    with _uncompressed_file(path) as source, xr.open_dataset(source, engine='netcdf4', mask_and_scale=False, cache=False) as raw:
        # Legacy files store floating fields with integer scale_factor=1. Force
        # floating CF decoding to preserve fractions and NaN fill values.
        for variable in raw.variables.values():
            for name in ('scale_factor', 'add_offset'):
                if name in variable.attrs:
                    variable.attrs[name] = float(variable.attrs[name])
        data = xr.decode_cf(raw)
        try:
            yield data
        finally:
            data.close()


def _operations(source):
    operations = {'interpolate': None, 'split': None}
    supplied = source.get('operations', {})
    if not isinstance(supplied, dict) or set(supplied) - operations.keys():
        raise ValueError('Source operations must be a dictionary containing interpolate and/or split.')
    operations.update(supplied)
    interpolation, split = operations['interpolate'], operations['split']
    if interpolation is not None:
        if (not isinstance(interpolation, dict) or set(interpolation) - {'method', 'roi'}
                or interpolation.get('method') not in ('nn', 'nearest', 'linear')):
            raise ValueError('interpolate must be null or {"method": "nn"/"nearest"/"linear", "roi": metres}.')
        roi = interpolation.get('roi')
        if roi is not None and (isinstance(roi, bool) or not isinstance(roi, (int, float))
                                or not np.isfinite(roi) or roi <= 0):
            raise ValueError('interpolate.roi must be null or a positive finite distance in metres.')
    if split is not None and (not isinstance(split, dict) or set(split) != {'method'}
                              or split['method'] != '3d_to_2d'):
        raise ValueError('split must be null or {"method": "3d_to_2d"}.')
    return operations


@contextmanager
def _source_dataset(cfg, path, ref):
    file_format = cfg.get('format', 'netcdf')
    if file_format == 'netcdf':
        with _open_dataset(path) as ds:
            yield ds
        return
    with _uncompressed_file(path) as source:
        if file_format in ('tiff', 'ascii'):
            import rasterio
            with rasterio.open(source) as raster:
                if raster.crs is not None and raster.crs.to_epsg() != 4326:
                    raise ValueError(f'{path}: raster CRS must be EPSG:4326.')
                if raster.transform.b or raster.transform.d:
                    raise ValueError(f'{path}: rotated raster grids are not supported.')
                band = cfg.get('band', 1)
                if isinstance(band, bool) or not isinstance(band, int) or not 1 <= band <= raster.count:
                    raise ValueError(f'{path}: invalid raster band.')
                if raster.count > 1 and 'band' not in cfg:
                    raise ValueError(f'{path}: configure band explicitly for a multiband raster.')
                values = raster.read(band, masked=True).astype(float).filled(np.nan)
                x = raster.transform.c + (np.arange(raster.width) + .5) * raster.transform.a
                y = raster.transform.f + (np.arange(raster.height) + .5) * raster.transform.e
        elif file_format == 'binary':
            binary = cfg.get('binary', {})
            if 'dtype' not in binary or 'order' not in binary or 'grid' not in cfg:
                raise ValueError(f'{path}: binary requires dtype, order and explicit grid metadata.')
            grid = cfg['grid']
            if grid == {'reference': 'terrain'}:
                x, y = ref.longitude.values, ref.latitude.values
            else:
                x, y = np.asarray(grid['x'], dtype=float), np.asarray(grid['y'], dtype=float)
            if x.ndim != 1 or y.ndim != 1 or not len(x) or not len(y):
                raise ValueError(f'{path}: binary grid x/y must be nonempty 1D arrays.')
            dtype, order, offset = np.dtype(binary['dtype']), binary['order'], binary.get('header_bytes', 0)
            if dtype.kind not in 'iuf' or order not in ('C', 'F'):
                raise ValueError(f'{path}: binary dtype must be numeric and order C or F.')
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise ValueError(f'{path}: binary.header_bytes must be a nonnegative integer.')
            expected = len(x) * len(y) * dtype.itemsize + offset
            if source.stat().st_size != expected:
                raise ValueError(f'{path}: binary byte count differs from the declared grid/dtype.')
            values = np.fromfile(source, dtype=dtype, offset=offset).reshape((len(y), len(x)), order=order).astype(float)
        else:
            raise ValueError(f'Unsupported source format {file_format!r}; use tiff, netcdf, binary or ascii.')
        x_name, y_name = cfg.get('x', 'longitude'), cfg.get('y', 'latitude')
        ds = xr.Dataset({cfg['variable']: ((y_name, x_name), values)}, coords={x_name: x, y_name: y})
        if cfg.get('units'):
            ds[cfg['variable']].attrs['units'] = cfg['units']
        try:
            yield ds
        finally:
            ds.close()


def _coordinates(data, cfg):
    info = read_geographical_info(data, cfg.get('x'), cfg.get('y'))
    return info['dims'], info['x'], info['y']


def _terrain(cfg, plan, time_tags=None):
    path = _path(cfg, plan, time_tags=time_tags)
    if cfg.get('format', 'netcdf') in ('ascii', 'tiff'):
        import rasterio
        with rasterio.open(path) as source:
            if source.crs is not None and source.crs.to_epsg() != 4326:
                raise ValueError('Terrain must use EPSG:4326.')
            if source.transform.b or source.transform.d:
                raise ValueError('Rotated terrain grids are not supported.')
            values = source.read(1, masked=True).astype(float).filled(np.nan)
            x = source.transform.c + (np.arange(source.width) + .5) * source.transform.a
            y = source.transform.f + (np.arange(source.height) + .5) * source.transform.e
    else:
        with _open_dataset(path) as ds:
            dims, xx, yy = _coordinates(ds, cfg)
            values = ds[cfg.get('variable', 'Terrain')].transpose(*dims).values.copy()
            if not (np.allclose(xx, xx[0]) and np.allclose(yy, yy[:, :1])):
                raise ValueError('Model terrain must have a regular rectilinear grid.')
            x, y = xx[0], yy[:, 0]
    if len(x) < 2 or len(y) < 2:
        raise ValueError('Terrain needs at least two cells along each axis.')
    ix, iy = np.argsort(x), np.argsort(y)[::-1]
    x, y, values = x[ix], y[iy], values[np.ix_(iy, ix)]
    if not (np.allclose(np.diff(x), x[1]-x[0]) and np.allclose(np.diff(y), y[1]-y[0])
            and np.isclose(x[1]-x[0], y[0]-y[1]) and x[1] > x[0] and y[1] < y[0]):
        raise ValueError('Model grid axes must be unique, regularly spaced and use square cells.')
    values = np.asarray(values, dtype=float)
    values[values == cfg.get('nodata', -9999)] = np.nan
    values[~np.isfinite(values)] = np.nan
    ref = xr.DataArray(values, dims=('latitude', 'longitude'),
                       coords={'latitude': y, 'longitude': x})
    if not np.isfinite(values).any():
        raise ValueError('Terrain has no valid cells.')
    ref.attrs.update(ncols=len(x), nrows=len(y), xllcorner=float(x.min() - (x[1]-x[0])/2),
                     yllcorner=float(y.min() - (y[0]-y[1])/2), cellsize=float(x[1]-x[0]),
                     NODATA_value=-9999.0)
    return ref


def _read_field(cfg, path, time, ref, operations, units, dataset=None):
    with nullcontext(dataset) if dataset is not None else _source_dataset(cfg, path, ref) as ds:
        variable = cfg['variable']
        if variable not in ds:
            return None, 'variable missing'
        field = ds[variable]
        time_name = cfg.get('time', 'time')
        keep_time_dimension = time_name in field.dims and operations['split'] is None
        if time_name in field.dims:
            if time_name not in ds.coords:
                raise ValueError(f'{path}: time dimension has no coordinate.')
            times = pd.DatetimeIndex(ds[time_name].values)
            if not times.is_unique or times.hasnans:
                raise ValueError(f'{path}: invalid/duplicate time coordinates.')
            if time not in times:
                return None, 'timestamp missing'
            if field.sizes[time_name] > 1 and operations['split'] is None:
                raise ValueError(f'{path}: configure split.method="3d_to_2d" for multiple timestamps.')
            field = split_3d_to_2d(field, time, time_name) if operations['split'] is not None else field.isel({time_name: times.get_loc(time)})
        elif time_name in ds:
            times = pd.to_datetime(np.asarray(ds[time_name]).reshape(-1))
            if len(times) != 1 or times[0] != time:
                return None, 'timestamp missing'
        # Untimed 2D files use the timestamp encoded in their configured filename.
        geography = check_geographical_info(ds, ref, cfg.get('x'), cfg.get('y'))
        dims, x, y = (geography['source'][name] for name in ('dims', 'x', 'y'))
        if set(field.dims) != set(dims):
            raise ValueError(f'{path}: variable must be a spatial 2D field after time selection.')
        values = field.transpose(*dims).values.astype(float)
        found_units = field.attrs.get('units')
        if found_units and found_units != units and not ('scale' in cfg or 'offset' in cfg):
            raise ValueError(f'{path}: units {found_units!r} differ from {units!r}; configure scale/offset.')
        if 'nodata' in cfg:
            values[values == cfg['nodata']] = np.nan
        values = values * cfg.get('scale', 1.) + cfg.get('offset', 0.)
    if geography['same_grid']:
        values = values[np.ix_(geography['y_index'], geography['x_index'])]
    else:
        if operations['interpolate'] is None:
            raise ValueError(f'{path}: grid differs from terrain; configure this variable interpolate operation.')
        values = resample_data(values, x, y, geography['expected']['x'], geography['expected']['y'],
                               **operations['interpolate'])
    active = np.isfinite(ref.values)
    missing_count = int((~np.isfinite(values[active])).sum())
    if missing_count:
        return None, f'missing/nonfinite data on active terrain cells ({missing_count} cells)'
    values[~active] = np.nan
    return values[np.newaxis] if keep_time_dimension else values, 'available'


def _write_step(path, fields, ref, time, model, variables):
    if model == 'hmc':
        from shybox.io_toolkit.lib_io_nc_hmc import write_dataset_hmc
        write_dataset_hmc(str(path), fields, ref, time=time, force_3d=False,
                          file_compression=False,
                          var_units={v['output_name']: v['units'] for v in variables.values()})
    else:
        from shybox.io_toolkit.lib_io_nc_s3m import write_dataset_s3m
        ds = xr.Dataset({key: (('Y', 'X') if value.ndim == 2 else ('time', 'Y', 'X'), value)
                         for key, value in fields.items()},
                        coords={'X': ref.longitude.values, 'Y': ref.latitude.values, 'time': [time]})
        # Float scale factors avoid integer decoding/truncation in xarray.
        attrs = {v['output_name']: {'units': v['units']} for v in variables.values()}
        attrs['scale_factor'] = 1.0
        write_dataset_s3m(str(path), ds, time=time, ref=ref, file_compression=False,
                          attrs_data=attrs, attrs_x={'scale_factor': 1.0}, attrs_y={'scale_factor': 1.0})


def _validate_step(path, fields, ref, time, model, variables):
    dims = ('south_north', 'west_east') if model == 'hmc' else ('Y', 'X')
    with _open_dataset(path) as ds:
        if ds.sizes.get('time') != 1 or pd.Timestamp(ds.time.values[0]) != time:
            raise ValueError(f'Invalid output time: {path}')
        x, y = np.meshgrid(ref.longitude.values, ref.latitude.values[::-1])
        np.testing.assert_allclose(ds.longitude, x, atol=1e-6, rtol=0)
        np.testing.assert_allclose(ds.latitude, y, atol=1e-6, rtol=0)
        for name, values in fields.items():
            expected = dims if values.ndim == 2 else ('time', *dims)
            if ds[name].dims != expected or not np.issubdtype(ds[name].dtype, np.floating):
                raise ValueError(f'Invalid output variable dimensions/type: {name}')
            actual = ds[name].values
            np.testing.assert_allclose(actual, np.flip(values, axis=-2), rtol=1e-5, atol=1e-6, equal_nan=True)
        for cfg in variables.values():
            if ds[cfg['output_name']].attrs.get('units') != cfg['units']:
                raise ValueError(f'Invalid output units: {cfg["output_name"]}')


def plan_datasets_probabilistic(settings):
    """Declare one common member range; deterministic fields are shared by members."""
    members = None
    for key, source in ((f'{group}.{key}', source)
                        for group in ('observed', 'forecast')
                        for key, source in settings['sources'][group].items()):
        source_mode = DatasetMode.from_source(source)
        has_member = '{ensemble_member}' in json.dumps(source)
        if not source_mode.is_probabilistic:
            if has_member:
                raise ValueError(f'{key}: a deterministic variable cannot have member-dependent sources.')
            continue
        spec = source.get('ensemble')
        if not isinstance(spec, dict):
            raise ValueError(f'{key}: probabilistic mode requires ensemble start, end and format.')
        start, end = spec.get('start'), spec.get('end')
        if (isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, int)
                or not isinstance(end, int) or start < 0 or end < start):
            raise ValueError(f'{key}: ensemble start/end must define a nonnegative inclusive integer range.')
        if not isinstance(spec.get('format'), str) or not spec['format']:
            raise ValueError(f'{key}: declare an ensemble format such as "03d".')
        declared = [format(member, spec['format']) for member in range(start, end + 1)]
        if len(set(declared)) != len(declared) or any(not value.isdigit() for value in declared):
            raise ValueError(f'{key}: ensemble format must produce unique numeric member IDs.')
        if not has_member:
            raise ValueError(f'{key}: probabilistic sources require an {{ensemble_member}} placeholder.')
        if members is not None and members != declared:
            raise ValueError('Probabilistic variables must declare the same ordered ensemble range and format.')
        members = declared
    mode = 'probabilistic_ensemble' if members is not None else 'deterministic'
    if settings.get('run_mode', mode) != mode:
        raise ValueError('Run mode must match the modes declared for its variables.')
    return mode, members or []


def compose_datasets(settings, time_run, plan=None):
    """Compose deterministic forcing or atomically publish a complete ensemble."""
    plan = plan_datasets_deterministic(settings, time_run) if plan is None else plan
    mode, members = plan_datasets_probabilistic(settings)
    if not members:
        return _compose_member(settings, time_run, plan=plan)
    time_tags = settings.get('time_tags', {})
    root = _path({'folder_name': settings['destination']['folder_name']}, plan, time_tags=time_tags)
    if root.exists():
        raise FileExistsError(f'Forcing run folder already exists: {root}')
    root.parent.mkdir(parents=True, exist_ok=True)
    template = settings['destination'].get('ensemble_folder', 'run_{ensemble_member}')
    if '{ensemble_member}' not in template:
        raise ValueError('ensemble_folder must contain {ensemble_member}.')
    with tempfile.TemporaryDirectory(prefix='.forcing-ensemble-', dir=root.parent) as temp:
        staging = Path(temp) / 'run'
        staging.mkdir()
        folders = {}
        for member in members:
            relative = _path({'file_name': template}, plan, time_tags=time_tags, ensemble_member=member)
            if relative.is_absolute() or '..' in relative.parts or relative == Path('.') or relative.name == 'manifest.json':
                raise ValueError('ensemble_folder must stay inside the forcing run folder.')
            member_settings = deepcopy(settings)
            member_settings['destination']['folder_name'] = str(staging / relative)
            _compose_member(member_settings, time_run, plan=plan, ensemble_member=member)
            folders[member] = str(relative)
        manifest = {**plan.as_dict(), 'status': 'complete', 'model': settings.get('model', 'hmc'),
                    'run_mode': mode, 'ensemble_members': members, 'member_folders': folders}
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        if root.exists():
            raise FileExistsError(f'Forcing run folder appeared during composition: {root}')
        staging.rename(root)
    return root


def _compose_member(settings, time_run, plan=None, ensemble_member=None):
    """Validate and stage a complete new run, then publish its folder atomically.

    Existing forcing folders are never replaced. Missing mandatory data at the
    first timestamp of *each* source group always fails, regardless of tolerance.
    """
    plan = plan_datasets_deterministic(settings, time_run) if plan is None else plan
    if not isinstance(plan, ForcingPlan):
        raise TypeError('plan must be a ForcingPlan computed by the calling workflow.')
    time_tags = settings.get('time_tags', {})
    variables, options = settings['variables'], settings.get('options', {})
    model = settings.get('model', 'hmc')
    if model not in ('hmc', 's3m'):
        raise ValueError('model must be hmc or s3m.')
    if not MANDATORY <= variables.keys():
        raise ValueError(f'Missing mandatory variable definitions: {sorted(MANDATORY - variables.keys())}')
    names = [v['output_name'] for v in variables.values()]
    if len(names) != len(set(names)) or set(names) & {'longitude', 'latitude', 'time', 'crs', 'terrain'}:
        raise ValueError('Output variable names must be unique and not reserved.')
    for key, variable in variables.items():
        if not variable.get('units'):
            raise ValueError(f'{key}: output units are required.')
    if set(options) & {'interpolate', 'interpolation_method', 'split_3d_to_2d'}:
        raise ValueError('Configure interpolate/split in each source operations dictionary, not workflow.options.')
    operations = {}
    for group in ('observed', 'forecast'):
        operations[group] = {}
        for key, variable in variables.items():
            source = settings['sources'][group].get(key, {})
            tolerance = source.get('tolerance_steps', 0)
            if isinstance(tolerance, bool) or not isinstance(tolerance, int) or tolerance < 0:
                raise ValueError(f'{group}.{key}: tolerance_steps must be a nonnegative integer.')
            if source.get('missing_policy', 'error') not in ('error', 'persist', 'nodata'):
                raise ValueError(f'{group}.{key}: missing_policy must be error, persist or nodata.')
            if source.get('format', 'netcdf') not in ('tiff', 'netcdf', 'binary', 'ascii'):
                raise ValueError(f'{group}.{key}: accepted formats are tiff, netcdf, binary and ascii.')
            operations[group][key] = _operations(source)
            if not isinstance(source.get('as_is', False), bool):
                raise ValueError(f'{group}.{key}: as_is must be true or false.')
            if source.get('as_is', False):
                if source.get('format', 'netcdf') != 'netcdf' or any(operations[group][key].values()):
                    raise ValueError(f'{group}.{key}: as_is requires NetCDF without split/interpolate operations.')
                if source.get('scale', 1) != 1 or source.get('offset', 0) != 0:
                    raise ValueError(f'{group}.{key}: as_is cannot change values.')
                if source.get('variable') != variable['output_name']:
                    raise ValueError(f'{group}.{key}: as_is cannot rename variables.')
        flags = [settings['sources'][group].get(key, {}).get('as_is', False) for key in variables]
        if any(flags) and not all(flags):
            raise ValueError(f'{group}: as_is requires all destination variables from one model-ready file.')
    ref = _terrain(settings['geo'], plan, time_tags=time_tags)
    destination = _path({'folder_name': settings['destination']['folder_name']}, plan, time_tags=time_tags)
    if destination.exists():
        raise FileExistsError(f'Forcing run folder already exists: {destination}')
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest = {**plan.as_dict(), 'model': model, 'status': 'complete',
                'variables': variables, 'options': options, 'time_tags': time_tags,
                'run_mode': 'probabilistic_ensemble' if ensemble_member is not None else 'deterministic',
                'ensemble_member': ensemble_member, 'files': []}
    with tempfile.TemporaryDirectory(prefix='.forcing-stage-', dir=destination.parent) as temp, ExitStack() as source_stack:
        staging = Path(temp) / 'run'
        staging.mkdir()
        outputs = set()
        uncompressed = {}
        datasets = {}
        for group, times in (('observed', plan.observed), ('forecast', plan.forecast)):
            previous, gaps = {}, {}
            for index, time in enumerate(times):
                LOG.info('Compose %s forcing at %s [member=%s]', group, time, ensemble_member or 'deterministic')
                fields, provenance = {}, {}
                for key, variable in variables.items():
                    source = settings['sources'][group].get(key)
                    cfg = source or {}
                    source_mode = DatasetMode.from_source(cfg)
                    if source_mode.is_probabilistic:
                        source_mode = source_mode.for_member(ensemble_member)
                    mandatory = key in MANDATORY or cfg.get('required', False)
                    values, reason, path = None, 'source not configured', None
                    if source:
                        path = _path(source, plan, time, time_tags=time_tags, ensemble_member=ensemble_member)
                        if path.exists():
                            read_path = path
                            if path.suffix == '.gz':
                                if path not in uncompressed:
                                    uncompressed[path] = source_stack.enter_context(_uncompressed_file(path))
                                read_path = uncompressed[path]
                            dataset = None
                            if source.get('format', 'netcdf') == 'netcdf':
                                if read_path not in datasets:
                                    datasets[read_path] = source_stack.enter_context(_open_dataset(read_path))
                                dataset = datasets[read_path]
                            values, reason = _read_field(source, read_path, time, ref, operations[group][key], variable['units'], dataset=dataset)
                        else:
                            reason = 'file missing'
                    status = 'available'
                    data_time, data_source = str(time), str(path) if path else None
                    if values is None:
                        gaps[key] = gaps.get(key, 0) + 1
                        policy = cfg.get('missing_policy', 'error')
                        if mandatory and (index == 0 or gaps[key] > cfg.get('tolerance_steps', 0) or policy == 'error'):
                            requirement = 'first step is mandatory' if index == 0 else 'gap policy/tolerance does not permit missing data'
                            raise ValueError(f'{group} {time} {key}: {reason}; {requirement}, gaps={gaps[key]}.')
                        if policy == 'persist' and gaps[key] <= cfg.get('tolerance_steps', 0) and key in previous:
                            last_values, data_time, data_source = previous[key]
                            values, status = last_values.copy(), 'persisted'
                        else:
                            shape = previous[key][0].shape if key in previous else ref.shape
                            values, status = np.full(shape, np.nan), 'nodata'
                            data_time, data_source = None, None
                        LOG.warning('%s %s %s: %s (%s)', group, time, key, status, reason)
                    else:
                        previous[key], gaps[key] = (values.copy(), data_time, data_source), 0
                    fields[variable['output_name']] = values
                    provenance[key] = {'source': str(path) if path else None, 'status': status, 'reason': reason,
                                       'data_time': data_time, 'data_source': data_source,
                                       'format': source.get('format', 'netcdf') if source else None,
                                       'operations': operations[group][key], **source_mode.as_dict()}
                relative = _path({'file_name': _destination_filename(settings['destination'], group)}, plan, time, time_tags=time_tags)
                if relative.is_absolute() or '..' in relative.parts or relative in outputs or relative.name == 'manifest.json':
                    raise ValueError(f'Output path must be unique and inside forcing folder: {relative}')
                if relative.suffix not in ('.nc', '.gz') or (relative.suffix == '.gz' and not str(relative).endswith('.nc.gz')):
                    raise ValueError('Output filenames must end with .nc or .nc.gz.')
                outputs.add(relative)
                path = staging / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                nc_path = path.with_suffix('') if path.suffix == '.gz' else path
                as_is = all(settings['sources'][group].get(key, {}).get('as_is', False) for key in variables)
                if as_is:
                    source_paths = {record['source'] for record in provenance.values()}
                    if len(source_paths) != 1 or any(record['status'] != 'available' for record in provenance.values()):
                        raise ValueError('as_is requires one complete model-ready source file at this timestamp.')
                    original = Path(next(iter(source_paths)))
                    copy_as_is(uncompressed.get(original, original), nc_path)
                else:
                    _write_step(nc_path, fields, ref, time, model, variables)
                _validate_step(nc_path, fields, ref, time, model, variables)
                if path.suffix == '.gz':
                    with nc_path.open('rb') as reader, gzip.open(path, 'wb') as writer:
                        shutil.copyfileobj(reader, writer)
                    nc_path.unlink()
                manifest['files'].append({'time': str(time), 'group': group, 'path': str(relative), 'as_is': as_is, 'variables': provenance})
        if not manifest['files']:
            raise ValueError('The configured time window contains no forcing steps.')
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        if destination.exists():
            raise FileExistsError(f'Forcing run folder appeared during composition: {destination}')
        staging.rename(destination)
    return destination
