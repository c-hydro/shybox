"""Run Ruler's complete native workspace/CSV/figure reporting workflow."""
import argparse
import json
from shybox.monitoring_toolkit.resources.ruler import RulerMonitor, load_ruler_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='workflow/monitoring/ruler_native.example.json')
    parser.add_argument('--time', help='Reference time for report names.')
    args = parser.parse_args()
    print(json.dumps(RulerMonitor(load_ruler_config(args.config), report_time=args.time).run(), indent=2))


if __name__ == '__main__':
    main()
