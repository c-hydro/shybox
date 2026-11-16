"""Source-mode views of the current adjusted file plan; no extra file probes."""
from copy import deepcopy


class PlanningModeHandler:
    """Small extension mixed into the public DatasetPlanner."""

    def file_plan_by_mode(self, mode, *, time_mode=None, member=None, unique=True):
        """Filter source mode independently of observed/forecast time mode.

        Deterministic rows repeated for member destinations are collapsed when
        ``unique=True``. Their retained destination is only representative; use
        ``unique=False`` for publishing member outputs. The full plan is unchanged.
        """
        if mode not in ('deterministic', 'probabilistic'):
            raise ValueError('mode must be deterministic or probabilistic.')
        frame = self.file_plan
        mask = frame['mode'].eq('deterministic') if mode == 'deterministic' else frame['mode'].isin(
            ('probabilistic', 'probabilistic_ensemble'))
        if time_mode is not None:
            if time_mode not in self.GROUPS:
                raise ValueError('time_mode must be observed, forecast or None.')
            mask &= frame.time_mode.eq(self.GROUPS[time_mode])
        if member is not None:
            if member not in (self.members or ('deterministic',)):
                raise ValueError(f'Unknown member {member!r}.')
            mask &= frame.output_member.isna() | frame.output_member.eq(member)
        selected = frame.loc[mask].copy(deep=True)
        if unique and mode == 'deterministic':
            selected = selected.drop_duplicates([
                'time', 'group', 'variable', 'collection', 'source_name', 'output_name', 'file_source'])
        return selected.reset_index(drop=True)

    def source_settings(self, variable, time_mode):
        """Return an independent source configuration for a canonical variable."""
        return deepcopy(self._sources[self.GROUPS[time_mode]][variable][1])

    def destination_settings(self, variable, time_mode):
        """Return an independent destination collection configuration."""
        return deepcopy(self._destinations[self.GROUPS[time_mode]][variable][2])
