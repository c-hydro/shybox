"""Sample process RSS and host RAM into bounded snapshots and daily CSV files."""
import csv
import json
import math
import os
from pathlib import Path
import socket
import time

from ..common.io import time_string, utc_now, write_status


def load_memory_config(file_name):
    path = Path(file_name).resolve()
    cfg = json.loads(path.read_text()).get('memory_monitoring')
    if not isinstance(cfg, dict) or not isinstance(cfg.get('processes'), dict) or not cfg['processes']:
        raise ValueError('memory_monitoring.processes must declare at least one process.')
    interval = cfg.setdefault('interval_seconds', 5)
    if not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval <= 0:
        raise ValueError('interval_seconds must be positive.')
    for label, spec in cfg['processes'].items():
        if not isinstance(spec, dict) or sum(key in spec for key in ('pid', 'name', 'cmdline_contains')) != 1:
            raise ValueError(f'{label}: select exactly one of pid, name or cmdline_contains.')
        if 'pid' in spec and (type(spec['pid']) is not int or spec['pid'] <= 0):
            raise ValueError(f'{label}: pid must be a positive integer.')
        for key in ('name', 'cmdline_contains'):
            if key in spec and (not isinstance(spec[key], str) or not spec[key].strip()):
                raise ValueError(f'{label}: {key} must be a nonempty string.')
        warning, critical = spec.get('warning_rss_mib', 2048), spec.get('critical_rss_mib', 4096)
        if not 0 < warning < critical or not all(math.isfinite(value) for value in (warning, critical)):
            raise ValueError(f'{label}: require 0 < warning_rss_mib < critical_rss_mib.')
    output = cfg.setdefault('output', {})
    output['folder_name'] = str((path.parent / Path(output.get('folder_name', '../../exec/monitoring_ram')).expanduser()).resolve())
    return cfg


class ProcessMemoryMonitor:
    """Name matches are exact; cmdline matches are case-sensitive substrings.

    Child RSS is summed once per target. Shared pages can be counted by more
    than one process; summed RSS is not unique physical-memory consumption.
    """
    def __init__(self, config):
        try:
            import psutil
        except ImportError as exc:
            raise ImportError('Process RAM monitoring requires psutil; install shybox[monitoring].') from exc
        self.psutil = psutil
        self.config = config
        self.folder = Path(config['output']['folder_name'])
        self.peaks = {}
        self.pid_births = {}

    def sample(self):
        psutil = self.psutil
        vm = psutil.virtual_memory()
        snapshot = {'schema_version': 1, 'resource': 'memory', 'generated_at': time_string(utc_now()),
                    'host': {'name': socket.gethostname(), 'memory_total_mib': vm.total / 2**20,
                             'memory_available_mib': vm.available / 2**20,
                             'memory_used_mib': (vm.total - vm.available) / 2**20,
                             'memory_used_percent': 100 * (vm.total - vm.available) / vm.total}, 'targets': []}
        for label, spec in self.config['processes'].items():
            roots, errors = [], []
            if 'pid' in spec:
                try:
                    process = psutil.Process(spec['pid'])
                    birth = process.create_time()
                    self.pid_births.setdefault(label, birth)
                    if birth == self.pid_births[label]:
                        roots.append(process)
                except psutil.NoSuchProcess:
                    pass
                except psutil.AccessDenied:
                    errors.append('Access denied to target PID.')
            else:
                for process in psutil.process_iter(['pid', 'name']):
                    if process.pid == os.getpid():
                        continue
                    try:
                        matches = (process.info['name'] == spec['name']) if 'name' in spec else (
                            spec['cmdline_contains'] in ' '.join(process.cmdline()))
                        if matches:
                            roots.append(process)
                    except psutil.NoSuchProcess:
                        continue
                    except psutil.AccessDenied:
                        # Unrelated inaccessible processes cannot establish a match.
                        continue
            processes = {process.pid: process for process in roots}
            if spec.get('include_children', False):
                for process in roots:
                    try:
                        processes.update({child.pid: child for child in process.children(recursive=True)})
                    except psutil.NoSuchProcess:
                        pass
                    except psutil.AccessDenied:
                        errors.append(f'Access denied to children of PID {process.pid}.')
            rss, vms, sampled = 0, 0, []
            for process in processes.values():
                try:
                    with process.oneshot():
                        if process.status() == psutil.STATUS_ZOMBIE:
                            continue
                        memory = process.memory_info()
                        sampled.append({'pid': process.pid, 'name': process.name()})
                        rss += memory.rss; vms += memory.vms
                except psutil.NoSuchProcess:
                    continue
                except psutil.AccessDenied:
                    errors.append(f'Access denied to memory of PID {process.pid}.')
            rss_mib = rss / 2**20 if sampled else None
            if rss_mib is not None:
                self.peaks[label] = max(self.peaks.get(label, 0), rss_mib)
            warning, critical = spec.get('warning_rss_mib', 2048), spec.get('critical_rss_mib', 4096)
            level = 'unknown' if rss_mib is None else 'critical' if rss_mib >= critical else 'warning' if rss_mib >= warning else 'normal'
            snapshot['targets'].append({'label': label, 'status': 'partial' if sampled and errors else 'running' if sampled else 'unavailable' if errors else 'missing',
                                        'rss_mib': rss_mib, 'vms_mib': vms / 2**20 if sampled else None,
                                        'peak_rss_mib': self.peaks.get(label), 'level': level,
                                        'process_count': len(sampled), 'processes': sampled, 'errors': errors})
        ruler = self.config.get('ruler', {})
        if ruler.get('enabled', False):
            from .ruler import collect_ruler_report
            snapshot['ruler'] = collect_ruler_report(snapshot, disk_paths=ruler.get('disk_paths'),
                                                    process_memory_type=ruler.get('process_memory_type', 'partial'))
        return snapshot

    def write(self, snapshot):
        self.folder.mkdir(parents=True, exist_ok=True)
        write_status(self.folder / 'memory.latest.json', snapshot)
        date = snapshot['generated_at'][:10]
        path = self.folder / f'memory_{date}.csv'
        fields = ['timestamp', 'label', 'status', 'rss_mib', 'vms_mib', 'peak_rss_mib', 'level',
                  'process_count', 'pids', 'host_used_mib', 'host_total_mib', 'host_used_percent']
        new_file = not path.exists() or path.stat().st_size == 0
        with path.open('a', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            if new_file:
                writer.writeheader()
            for target in snapshot['targets']:
                writer.writerow({'timestamp': snapshot['generated_at'], **{key: target[key] for key in fields if key in target},
                                 'pids': ','.join(str(process['pid']) for process in target['processes']),
                                 'host_used_mib': snapshot['host']['memory_used_mib'],
                                 'host_total_mib': snapshot['host']['memory_total_mib'],
                                 'host_used_percent': snapshot['host']['memory_used_percent']})
        return path

    def run(self, *, samples=0):
        if samples < 0:
            raise ValueError('samples must be nonnegative; zero means continuous monitoring.')
        count = 0
        try:
            while samples == 0 or count < samples:
                snapshot = self.sample()
                self.write(snapshot)
                count += 1
                if samples == 0 or count < samples:
                    time.sleep(self.config['interval_seconds'])
        except KeyboardInterrupt:
            pass
        return count

