"""Run summaries adapted from HAT Analyzer_Execution's timing/reporting model.

HAT estimated execution timing from reference-file creation times. New records
use explicit lifecycle timestamps; legacy references remain an optional source
of availability information, never proof that a process exited successfully.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from ..common.configuration import resolve_monitoring_config

from ..common.io import read_status, time_string, utc_now


def analyze_status(record, *, stale_seconds=180, now=None):
    result = dict(record)
    now = now or utc_now()
    updated = datetime.fromisoformat(record['updated_at'].replace('Z', '+00:00'))
    if record['status'] == 'running' and (now - updated).total_seconds() > stale_seconds:
        result['status'] = 'stale'
    return result


def inspect_legacy_run(start_file, output_files):
    """Read HAT-style reference files without claiming an observed exit code."""
    def reference(path):
        path = Path(path)
        return {'path': str(path), 'available': path.is_file(),
                'updated_at': time_string(datetime.fromtimestamp(path.stat().st_mtime, timezone.utc))
                if path.is_file() else None}
    start = reference(start_file)
    outputs = [reference(path) for path in output_files]
    ended = max((item['updated_at'] for item in outputs if item['available']), default=None)
    elapsed = None
    if start['updated_at'] and ended:
        delta = (datetime.fromisoformat(ended.replace('Z', '+00:00')) -
                 datetime.fromisoformat(start['updated_at'].replace('Z', '+00:00'))).total_seconds()
        elapsed = delta if delta >= 0 else None
    return {'status': 'unknown', 'exit_code': None, 'start_reference': start, 'outputs': outputs,
            'output_status': ('available' if all(item['available'] for item in outputs) else 'missing')
            if outputs else 'unknown', 'estimated_duration_seconds': elapsed}


class MonitoringCatalog:
    """Read only declared applications from one configured monitoring folder."""
    def __init__(self, config):
        self.config = config or {}
        self.folder = Path(self.config['folder']) if self.config.get('folder') else None
        self.applications = self.config.get('applications', {})
        self.stale_seconds = self.config.get('stale_seconds', 180)
        if self.stale_seconds <= 0:
            raise ValueError('monitoring.stale_seconds must be positive.')
        if not isinstance(self.applications, dict):
            raise ValueError('monitoring.applications must be an object.')
        for spec in self.applications.values():
            if not isinstance(spec, dict) or not spec.get('run_name') or not spec.get('application'):
                raise ValueError('Each monitored application requires run_name and application.')
            members = spec.get('ensemble_members', [])
            if not isinstance(members, list) or len({str(member) for member in members}) != len(members):
                raise ValueError('ensemble_members must contain unique member identifiers.')
            if spec.get('expected_run_time'):
                time_string(spec['expected_run_time'])

    def summary(self, *, date=None, application=None, now=None):
        if date:
            datetime.strptime(date, '%Y-%m-%d')
        if application is not None and application not in self.applications:
            raise KeyError('Unknown monitored application.')
        issues, records = [], []
        if self.folder and self.folder.is_dir():
            for path in self.folder.rglob('*.json'):
                if not path.is_file() or not path.resolve().is_relative_to(self.folder.resolve()):
                    continue
                try:
                    record = read_status(path)
                    records.append(analyze_status(record, stale_seconds=self.stale_seconds, now=now))
                except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
                    issues.append(f'{path.name}: {exc}')
        rows, applications = [], []
        for key, spec in self.applications.items():
            if not spec.get('enabled', True) or (application and key != application):
                continue
            matched = [record for record in records
                       if record['run_name'] == spec['run_name'] and record['application'] == spec['application']
                       and all(record.get(field) == spec[field]
                               for field in ('run_domain', 'run_mode', 'ensemble') if field in spec)]
            if date:
                matched = [record for record in matched
                           if (record.get('run_time') or record['updated_at'])[:10] == date]
            matched.sort(key=lambda record: record['updated_at'], reverse=True)
            for record in matched:
                rows.append({**record, 'application_id': key})
            expected = time_string(spec.get('expected_run_time'))
            initialization = expected or max((record.get('run_time') for record in matched if record.get('run_time')), default=None)
            current = [record for record in matched if record.get('run_time') == initialization]
            # Each member's newest attempt contributes; retries do not double-count.
            latest = {}
            for record in current:
                latest.setdefault(str(record.get('ensemble')), record)
            counts = dict(Counter(record['status'] for record in latest.values()))
            members = spec.get('ensemble_members', [])
            if members:
                missing = sum(str(member) not in latest for member in members)
                if missing:
                    counts['missing'] = missing
            status = 'missing'
            if counts:
                status = next(item for item in ('failed', 'cancelled', 'stale', 'running',
                                                'missing', 'succeeded', 'skipped', 'dry_run') if counts.get(item))
            legacy = None
            if not matched and not date and spec.get('legacy'):
                try:
                    legacy = inspect_legacy_run(spec['legacy']['start_file'], spec['legacy'].get('output_files', []))
                    status = 'unknown'
                except OSError as exc:
                    issues.append(f'{key}: {exc}')
            applications.append({'id': key, 'name': spec.get('name', key), 'run_name': spec['run_name'],
                                 'application': spec['application'], 'status': status, 'counts': counts,
                                 'expected_run_time': expected, 'legacy': legacy, 'latest': current[0] if current else None})
        rows.sort(key=lambda record: record['updated_at'], reverse=True)
        return {'enabled': bool(self.folder), 'generated_at': time_string(now or utc_now()),
                'applications': applications, 'executions': rows, 'issues': issues,
                'counts': dict(Counter(item['status'] for item in applications))}
