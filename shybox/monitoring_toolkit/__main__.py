"""Produce a monitoring summary using a dashboard or standalone JSON config."""
import argparse
import json
from pathlib import Path

from . import MonitoringCatalog
from .common.io import write_status
from .common.configuration import resolve_monitoring_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', help='Summary JSON file; defaults to stdout')
    parser.add_argument('--date', help='Model initialization date, YYYY-MM-DD')
    args = parser.parse_args()
    path = Path(args.config).resolve()
    config = json.loads(path.read_text())
    config = resolve_monitoring_config(config.get('monitoring', config), path.parent)
    result = MonitoringCatalog(config).summary(date=args.date)
    if args.output:
        write_status(args.output, result)
    else:
        print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
