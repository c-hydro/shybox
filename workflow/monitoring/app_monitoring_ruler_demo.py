"""Start a bounded workload and generate Ruler workspaces, CSV and figures."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from shybox.monitoring_toolkit.resources.ruler import RulerMonitor, load_ruler_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('exec/monitoring_ruler_demo'))
    args = parser.parse_args()
    config_file = Path(__file__).with_name('ruler_native.example.json')
    config = load_ruler_config(config_file)
    output = args.output.resolve()
    for key in ('ancillary', 'destination', 'figure'):
        config['report'][key]['folder_name'] = str(output)
    process = subprocess.Popen([sys.executable, '-B', '-m', 'workflow.monitoring.app_monitoring_ruler_workload', '--seconds', '10'])
    try:
        result = RulerMonitor(config).run()
        print(json.dumps(result, indent=2))
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)


if __name__ == '__main__':
    main()
