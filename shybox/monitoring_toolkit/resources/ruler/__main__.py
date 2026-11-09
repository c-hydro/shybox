"""Run the integrated Ruler native JSON workflow."""
import argparse
import json
from .handler import RulerMonitor, load_ruler_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--time', help='Reference timestamp for report filenames.')
    args = parser.parse_args()
    print(json.dumps(RulerMonitor(load_ruler_config(args.config), report_time=args.time).run(), indent=2))


if __name__ == '__main__':
    main()
