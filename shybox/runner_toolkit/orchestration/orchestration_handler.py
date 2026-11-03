"""Generic orchestration for single and ensemble external-model runs."""
from __future__ import annotations

import copy
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    member: str | None
    member_index: int | None
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class RunResult:
    run_id: str
    member: str | None
    success: bool
    attempts: int
    elapsed_seconds: float
    output: Any = None
    error: str | None = None


def _replace_tags(obj: Any, tags: Mapping[str, str]) -> Any:
    """Replace only explicit orchestration placeholders, preserving HMC $yyyy etc."""
    if isinstance(obj, dict):
        return {k: _replace_tags(v, tags) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_tags(v, tags) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_replace_tags(v, tags) for v in obj)
    if isinstance(obj, str):
        for key, value in tags.items():
            obj = obj.replace('{' + key + '}', str(value))
        return obj
    return obj


class OrchestrationManager:
    """Schedule independently configured model runs using a user callback.

    Callback signature: callback(spec: RunSpec, config: dict) -> Any.
    The callback MUST raise on unsuccessful execution, or return an object
    checked by the callback itself. Thread parallelism is intended for
    independent external processes, not CPU-bound Python calculations.
    """

    def __init__(self, orchestration_obj: Mapping[str, Any],
                 execution_callback: Callable[[RunSpec, dict], Any],
                 logger_stream: logging.Logger | None = None):
        self.options = copy.deepcopy(dict(orchestration_obj))
        self.callback = execution_callback
        self.logger = logger_stream or logging.getLogger(__name__)
        self.simulation_mode = self.options.get('simulation_mode', 'deterministic')
        self.execution_strategy = self.options.get('execution_strategy', 'sequential')
        self.max_workers = self.options.get('max_workers', 1)
        self.max_retries = self.options.get('max_retries', 0)
        self.continue_on_error = self.options.get('continue_on_error', False)
        self._validate()
        self.specs = self._make_specs()
        self.results: dict[str, RunResult] = {}

    def _validate(self):
        if self.simulation_mode not in {'deterministic', 'probabilistic'}:
            raise ValueError('simulation_mode must be deterministic or probabilistic')
        if self.execution_strategy not in {'sequential', 'parallel'}:
            raise ValueError('execution_strategy must be sequential or parallel')
        for key, value, minimum in [('max_workers', self.max_workers, 1),
                                    ('max_retries', self.max_retries, 0)]:
            if type(value) is not int or value < minimum:
                raise ValueError(f'{key} must be an integer >= {minimum}')
        if type(self.continue_on_error) is not bool:
            raise ValueError('continue_on_error must be boolean')

    def _make_specs(self) -> list[RunSpec]:
        if self.simulation_mode == 'deterministic':
            return [RunSpec('deterministic', None, None, {'run_id': 'deterministic'})]
        members = self.options.get('members')
        if not isinstance(members, dict):
            raise ValueError('probabilistic mode requires members dictionary')
        fmt = members.get('format', '{member:03d}')
        if 'values' in members:
            values = members['values']
            if not isinstance(values, list) or not values:
                raise ValueError('members.values must be a nonempty list')
        else:
            start, end, step = (members.get('start'), members.get('end'), members.get('step', 1))
            if any(type(v) is not int for v in (start, end, step)) or step <= 0 or end < start:
                raise ValueError('invalid member start/end/step')
            values = list(range(start, end + 1, step))
        specs = []
        for value in values:
            if type(value) is int:
                member = fmt.format(member=value)
                index = value
            elif isinstance(value, str) and value:
                member = value
                index = int(value) if value.isdigit() else None
            else:
                raise ValueError('members must be integers or nonempty strings')
            if not re.fullmatch(r'[A-Za-z0-9_-]+', member):
                raise ValueError(f'unsafe member identifier: {member!r}')
            specs.append(RunSpec(member, member, index,
                                 {'run_id': member, 'ensemble_member': member,
                                  'ensemble_member_id': str(index) if index is not None else member}))
        if len({s.run_id for s in specs}) != len(specs):
            raise ValueError('duplicate member identifiers')
        return specs

    def configure(self, config: Mapping[str, Any], spec: RunSpec) -> dict:
        """Return a deep copy with orchestration tags resolved; never mutate input."""
        return _replace_tags(copy.deepcopy(dict(config)), spec.tags)

    def _execute(self, spec: RunSpec, config: Mapping[str, Any]) -> RunResult:
        start = time.monotonic()
        for attempt in range(1, self.max_retries + 2):
            try:
                configured = self.configure(config, spec)
                output = self.callback(spec, configured)
                self.logger.info('Run %s succeeded (attempt %d)', spec.run_id, attempt)
                return RunResult(spec.run_id, spec.member, True, attempt,
                                 time.monotonic() - start, output=output)
            except Exception as exc:
                self.logger.exception('Run %s failed (attempt %d)', spec.run_id, attempt)
                error = f'{type(exc).__name__}: {exc}'
        return RunResult(spec.run_id, spec.member, False, attempt,
                         time.monotonic() - start, error=error)

    def run(self, config: Mapping[str, Any]) -> dict[str, RunResult]:
        self.results = {}
        if self.execution_strategy == 'sequential':
            for spec in self.specs:
                result = self._execute(spec, config)
                self.results[spec.run_id] = result
                if not result.success and not self.continue_on_error:
                    raise RuntimeError(f'Run {spec.run_id} failed: {result.error}')
        else:
            # Submitted runs cannot be forcibly stopped once started.
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = {pool.submit(self._execute, spec, config): spec
                           for spec in self.specs}
                for future in as_completed(futures):
                    spec = futures[future]
                    self.results[spec.run_id] = future.result()
            failures = [r.run_id for r in self.results.values() if not r.success]
            if failures and not self.continue_on_error:
                raise RuntimeError(f'Failed runs: {", ".join(failures)}')
        return self.results

    def summary(self) -> dict:
        successes = [s.run_id for s in self.specs if s.run_id in self.results
                     and self.results[s.run_id].success]
        failures = [s.run_id for s in self.specs if s.run_id in self.results
                    and not self.results[s.run_id].success]
        return {'total': len(self.specs), 'completed': len(self.results),
                'successful': len(successes), 'failed': len(failures),
                'successful_runs': successes, 'failed_runs': failures}
