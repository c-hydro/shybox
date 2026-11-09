"""Run from the repository root: python -m workflow.monitoring.app_monitoring_memory_example --config workflow/monitoring/memory_monitoring.example.json --samples 6 --plot"""
import argparse
from pathlib import Path

from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor, load_memory_config, plot_memory_history
from shybox.monitoring_toolkit.common.io import utc_now


def main():
    parser = argparse.ArgumentParser(description='Collect process RAM using JSON configuration.')
    parser.add_argument('--config', required=True)
    parser.add_argument('--samples', type=int, default=0, help='Number of samples; zero runs until Ctrl+C.')
    parser.add_argument('--pid', type=int, help='Replace configured selectors with this single PID.')
    parser.add_argument('--plot', action='store_true', help='Write a RAM history PNG when sampling ends.')
    args = parser.parse_args()
    config = load_memory_config(args.config)
    if args.pid is not None:
        if args.pid <= 0:
            parser.error('--pid must be positive.')
        config['processes'] = {'selected_process': {'pid': args.pid, 'include_children': True}}
    monitor = ProcessMemoryMonitor(config)
    count = monitor.run(samples=args.samples)
    print(f'Collected {count} samples in {monitor.folder}')
    if args.plot and count:
        date = utc_now().date().isoformat()
        csv_file = monitor.folder / f'memory_{date}.csv'
        if csv_file.exists():
            print(plot_memory_history(csv_file, monitor.folder / 'memory.png'))


if __name__ == '__main__':
    main()
