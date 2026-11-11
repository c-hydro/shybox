"""Build observed/forecast periods and step/cube source reference times."""
from dataclasses import dataclass, field
import re

import pandas as pd


def _reference(run, hour):
    if hour is None:
        return run
    if not isinstance(hour, str) or re.fullmatch(r'\d{1,2}:\d{2}', hour.strip()) is None:
        raise ValueError('Reference hours must be null or HH:MM.')
    hours, minutes = map(int, hour.strip().split(':'))
    if hours > 23 or minutes > 59:
        raise ValueError('Reference hours must be between 00:00 and 23:59.')
    reference = run.normalize() + pd.Timedelta(hours=hours, minutes=minutes)
    # Select the most recent available cycle, never a future issue.
    return reference if reference <= run else reference - pd.Timedelta('1d')


def _horizon(boundary, horizon):
    if horizon is None:
        return None
    return boundary.floor('D' if str(horizon).strip().lower() == 'midnight' else str(horizon).strip())


def _select(look, horizon, priority):
    if priority not in ('look', 'horizon', 'min', 'max'):
        raise ValueError('Priority must be look, horizon, min or max.')
    if priority == 'look' or horizon is None and priority != 'horizon':
        return look
    if horizon is None:
        raise ValueError('Priority horizon requires a configured time_horizon.')
    if priority == 'horizon':
        return horizon
    return min(look, horizon) if priority == 'min' else max(look, horizon)


def _table(observed, forecast, observed_run, forecast_run, observed_mode='step', forecast_mode='3d'):
    if observed_mode not in ('step', '3d') or forecast_mode not in ('step', '3d'):
        raise ValueError('Reference mode must be step or 3d.')
    index = observed.append(forecast).rename('time')
    modes = ['obs'] * len(observed) + ['frc'] * len(forecast)
    cubes = [observed_run] * len(observed) + [forecast_run] * len(forecast)
    frame = pd.DataFrame({'time_mode': modes, 'time_reference_step': index,
                          'time_reference_3d': pd.DatetimeIndex(cubes)}, index=index)
    frame['time_reference'] = [
        cube if (observed_mode if mode == 'obs' else forecast_mode) == '3d' else stamp
        for stamp, mode, cube in zip(index, modes, cubes)]
    return frame


@dataclass(frozen=True)
class TimePeriod:
    time_run: pd.Timestamp
    forecast_run: pd.Timestamp
    observed: pd.DatetimeIndex
    forecast: pd.DatetimeIndex
    observed_run: pd.Timestamp | None = None
    table: pd.DataFrame | None = field(default=None, repr=False, compare=False)
    bounds: dict = field(default_factory=dict)
    frequency: pd.Timedelta | None = None

    def __post_init__(self):
        if self.observed_run is None:
            object.__setattr__(self, 'observed_run', self.time_run)
        if self.table is None:
            object.__setattr__(self, 'table', _table(self.observed, self.forecast, self.observed_run, self.forecast_run))
        table = self.table.sort_index().copy()
        frequency = self.frequency
        if frequency is None:
            offset = self.observed.freq or self.forecast.freq
            frequency = pd.Timedelta(offset.nanos, unit='ns') if offset is not None else None
        if frequency is None:
            raise ValueError('A time period requires a forcing frequency.')
        object.__setattr__(self, 'frequency', pd.Timedelta(frequency))
        table['step'] = range(1, len(table) + 1)
        table['time_step'] = ((table.index - self.time_run.floor(frequency)) // frequency).astype(int)
        if 'time_checked' not in table:
            table['time_checked'] = False
        references = pd.MultiIndex.from_arrays([table.time_mode, table.time_reference])
        table['file_id'] = pd.factorize(references, sort=False)[0] + 1
        object.__setattr__(self, 'table', table)

    @property
    def time_period(self):
        return self.table.index

    def reference_time(self, time, mode='step'):
        if mode not in ('step', '3d'):
            raise ValueError('Reference mode must be step or 3d.')
        return self.table.loc[pd.Timestamp(time), 'time_reference_' + mode]

    def as_dict(self):
        frame = self.table.reset_index().copy()
        for column in frame.columns:
            if pd.api.types.is_datetime64_any_dtype(frame[column]):
                frame[column] = frame[column].astype(str)
        frame = frame.astype(object).where(frame.notna(), None)
        return {'time_run': str(self.time_run), 'observed_run': str(self.observed_run),
                'forecast_run': str(self.forecast_run),
                'time_period': [str(t) for t in self.time_period],
                'observed': [str(t) for t in self.observed],
                'forecast': [str(t) for t in self.forecast],
                'bounds': self.bounds, 'time_table': frame.to_dict(orient='records')}


def create_time_period(
        time_run, time_rounding='1h', time_frequency='1h',
        hour_reference_observed=None, hour_reference_forecast='00:00',
        time_look_observed='48h', time_horizon_observed='midnight',
        time_look_forecast='72h', time_horizon_forecast='midnight',
        priority_observed='min', priority_forecast='min',
        reference_mode_observed='step', reference_mode_forecast='3d'):
    """Choose observed starts/forecast ends from look and horizon candidates.

    Look is measured from the rounded run. The horizon candidate is measured
    from the configured cycle and then floored to midnight (or a fixed offset).
    Observed endpoints are inclusive; forecasts start one step after the cutoff.
    """
    run = pd.Timestamp(time_run)
    if pd.isna(run) or run.tzinfo is not None:
        raise ValueError('time_run must be a valid naive UTC timestamp.')
    run = run.floor(str(time_rounding).strip())
    frequency = pd.Timedelta(str(time_frequency).strip())
    back, ahead = pd.Timedelta(str(time_look_observed).strip()), pd.Timedelta(str(time_look_forecast).strip())
    if pd.isna(frequency) or frequency <= pd.Timedelta(0) or pd.isna(back) or pd.isna(ahead) or min(back, ahead) < pd.Timedelta(0):
        raise ValueError('Frequency must be positive and look durations nonnegative.')
    observed_run, forecast_run = _reference(run, hour_reference_observed), _reference(run, hour_reference_forecast)
    look_start, look_end = run - back, run + ahead
    horizon_start = _horizon(observed_run - back, time_horizon_observed)
    horizon_end = _horizon(forecast_run + ahead, time_horizon_forecast)
    start = _select(look_start, horizon_start, priority_observed).ceil(frequency)
    end = _select(look_end, horizon_end, priority_forecast).floor(frequency)
    cutoff = run.floor(frequency)
    observed = pd.date_range(start, cutoff, freq=frequency)
    forecast = pd.date_range(cutoff + frequency, end, freq=frequency)
    table = _table(observed, forecast, observed_run, forecast_run, reference_mode_observed, reference_mode_forecast)
    bounds = {
        'observed': {'look': str(look_start), 'horizon': str(horizon_start) if horizon_start is not None else None,
                     'selected': str(start), 'priority': priority_observed},
        'forecast': {'look': str(look_end), 'horizon': str(horizon_end) if horizon_end is not None else None,
                     'selected': str(end), 'priority': priority_forecast}}
    return TimePeriod(run, forecast_run, observed, forecast, observed_run, table, bounds, frequency)
