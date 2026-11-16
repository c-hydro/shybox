#!/usr/bin/env python3
"""Opt-in parallel variant. The existing sequential workflow is unchanged."""
import argparse
import json
from pathlib import Path
import sys

# Also support direct execution of this new script from any working directory.
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from workflow.runner.datasets_parallel.pipeline import load_settings, prepare_run, run_parallel_datasets
else:
    from .pipeline import load_settings, prepare_run, run_parallel_datasets


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-settings_file', '--settings-file', required=True)
    parser.add_argument('-time', '--time', dest='time_run')
    parser.add_argument('--run-dir', help='New directory for this variant only; must not already exist.')
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--native-threads', type=int, default=1)
    parser.add_argument('--task-unit', choices=('auto', 'member', 'time'), default='auto')
    parser.add_argument('--time-chunk', type=int, default=1)
    parser.add_argument('--time-mode', choices=('observed', 'forecast', 'all'), default='forecast')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--execute', action='store_true', help='Actually process data using worker processes.')
    mode.add_argument('--dry-run', action='store_true', help='Plan only (the default).')
    args = parser.parse_args(argv)
    if args.workers < 1 or args.native_threads < 1:
        parser.error('--workers and --native-threads must be positive.')
    application, workflow, tags = load_settings(args.settings_file)
    prepared = prepare_run(
        application, workflow, run_dir=args.run_dir, time_run=args.time_run, time_tags=tags,
        time_mode=args.time_mode, task_unit=args.task_unit, time_chunk=args.time_chunk,
    )
    result = prepared.report()
    result['dry_run'] = not args.execute
    if args.execute:
        result = run_parallel_datasets(prepared, max_workers=args.workers, native_threads=args.native_threads)
    print(json.dumps(result, indent=2, default=str))
    return 1 if result.get('status') == 'failed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
