"""Generate Hydrapp demo statuses and keep an analyzer heartbeat alive.

python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources
Open Hydrapp with --config hydrapp/config.demo.json in another terminal.
"""
import argparse
from datetime import timedelta
from pathlib import Path
import time

from shybox.monitoring_toolkit.execution import RunMonitor
from shybox.monitoring_toolkit.common.io import time_string, utc_now, write_status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    parser.add_argument('--folder-name', type=Path, default=root / 'hydrapp/demo/monitoring')
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--resources', action='store_true', help='Also sample this demo process with Ruler.')
    args = parser.parse_args()
    if args.duration < 0:
        parser.error('--duration must be nonnegative.')
    run_time = utc_now().replace(minute=0, second=0, microsecond=0)
    def monitor(application, **kwargs):
        obj = RunMonitor(args.folder_name, run_name='hydrapp_demo', application=application,
                         run_time=run_time, heartbeat_seconds=2, **kwargs)
        obj.record['demo'] = True
        return obj
    for application, code, status in [('converter', 0, 'succeeded'), ('hmc', 2, 'failed'),
                                      ('archive', None, 'skipped'), ('validation', None, 'dry_run')]:
        obj = monitor(application)
        if code is not None:
            obj.start()
        obj.finish(code, status=status, error='Synthetic input error for demonstration.' if code == 2 else None)
    stale = monitor('publisher')
    stale.finish(status='failed')
    stale.record.update(status='running', started_at=time_string(utc_now()-timedelta(minutes=10)),
                        updated_at=time_string(utc_now()-timedelta(minutes=5)), finished_at=None,
                        exit_code=None, duration_seconds=300)
    write_status(stale.file_path, stale.record)
    for member, code in [(1, 0), (2, 1)]:
        obj = monitor('ensemble_hmc', ensemble=member, run_mode='probabilistic').start()
        obj.finish(code, error='Synthetic member failure.' if code else None)
    live = monitor('analyzer').start()
    print(f'Demo records: {args.folder_name}\nLive analyzer record: {live.file_path}', flush=True)
    resources = None
    if args.resources:
        import os
        from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor, plot_memory_history
        resources = ProcessMemoryMonitor({'processes': {'status_demo': {'pid': os.getpid(), 'include_children': True}},
            'interval_seconds': 2, 'output': {'folder_name': str(root/'hydrapp/demo/memory')},
            'ruler': {'enabled': True, 'disk_paths': ['/'], 'process_memory_type': 'partial'}})
    try:
        deadline = time.monotonic() + args.duration
        while True:
            if resources:
                resources.write(resources.sample())
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(2, remaining))
    except KeyboardInterrupt:
        live.finish(130, status='cancelled', error='Demo interrupted.')
    except Exception as exc:
        live.finish(1, error=exc)
        raise
    finally:
        live.finish(0)
        if resources:
            csv_file = resources.folder/f'memory_{utc_now().date().isoformat()}.csv'
            if csv_file.exists():
                plot_memory_history(csv_file, resources.folder/'memory.png')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
