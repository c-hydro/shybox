"""Adapt Ruler's read-only collectors to SHYBOX monitoring snapshots."""
REVISION = '6fbbf6303620d1d9036a8176d405a3c66ae1d0bd'


def collect_ruler_report(snapshot, *, disk_paths=None, process_memory_type='partial'):
    import psutil
    from .engine.lib_analysis_fx import (
        organize_memory_info, organize_process_info, get_disk_info, get_system_load_info)
    from .engine.lib_analysis_utils import convert_obj2dict, convert_bytes2human_obj
    if process_memory_type not in ('partial', 'full'):
        raise ValueError('Ruler process_memory_type must be partial or full.')
    report = {'source': 'c-hydro/ruler', 'revision': REVISION,
              'memory': {}, 'disk': {}, 'load': {}, 'processes': {}, 'issues': []}
    try:
        report['memory'] = organize_memory_info(
            convert_bytes2human_obj(convert_obj2dict(psutil.virtual_memory())),
            convert_bytes2human_obj(convert_obj2dict(psutil.swap_memory())))
    except (OSError, psutil.Error) as exc:
        report['issues'].append(f'Memory: {exc}')
    try:
        report['load'] = get_system_load_info()
    except (OSError, AttributeError) as exc:
        report['issues'].append(f'Load: {exc}')
    for path in disk_paths or ['/']:
        try:
            report['disk'][str(path)] = get_disk_info(disk_path=str(path))
        except OSError as exc:
            report['issues'].append(f'Disk {path}: {exc}')
    attributes = ['pid', 'name', 'memory_info', 'memory_percent', 'cpu_percent', 'cpu_times', 'create_time', 'status']
    for target in snapshot['targets']:
        values = {}
        for process_info in target['processes']:
            try:
                # Use only PIDs already selected by SHYBOX; each report is a fresh observation.
                values.update(organize_process_info(
                    [psutil.Process(process_info['pid'])], prefix_name=f"pid_{process_info['pid']}_{{proc_n}}",
                    process_attributes=attributes, process_memory_type=process_memory_type))
            except (psutil.Error, OSError, AttributeError, ValueError) as exc:
                report['issues'].append(f"PID {process_info['pid']}: {exc}")
        report['processes'][target['label']] = values
    return report
