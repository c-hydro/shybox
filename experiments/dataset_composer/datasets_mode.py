"""Run mode and the currently selected forcing member."""
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class DatasetMode:
    mode: str = 'deterministic'
    member: str = 'deterministic'

    def __post_init__(self):
        mode = 'deterministic' if self.mode is None else self.mode
        if mode == 'probabilistic_ensemble':
            mode = 'probabilistic'
        if mode not in ('deterministic', 'probabilistic'):
            raise ValueError('Dataset mode must be deterministic or probabilistic.')
        member = 'deterministic' if self.member is None else str(self.member)
        if not member.strip():
            raise ValueError('A declared dataset member cannot be empty.')
        if mode == 'deterministic' and member != 'deterministic':
            raise ValueError('A deterministic source uses member "deterministic".')
        object.__setattr__(self, 'mode', mode)
        object.__setattr__(self, 'member', member)

    @property
    def is_probabilistic(self):
        return self.mode == 'probabilistic'

    @property
    def has_member(self):
        return self.is_probabilistic and self.member != 'deterministic'

    @property
    def run_mode(self):
        return 'probabilistic_ensemble' if self.is_probabilistic else 'deterministic'

    def for_member(self, member):
        return replace(self, member=member)

    @classmethod
    def from_source(cls, source, member=None):
        return cls(mode=source.get('mode', 'deterministic'),
                   member=member if member is not None else source.get('member', 'deterministic'))

    def as_dict(self):
        return {'mode': self.mode, 'member': self.member}
