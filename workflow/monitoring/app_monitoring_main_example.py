"""Example: explicitly monitor the start and end of a Python application.

Run from the SHYBOX repository root:
    python -m workflow.monitoring.app_monitoring_main_example
    python -m workflow.monitoring.app_monitoring_main_example --fail

The defaults match the dashboard's Marche HMC declarations. This is a demo
workload, not a model execution. Use --folder-name /tmp/shybox-monitoring-demo
to keep demonstration records separate from operational monitoring.
"""
import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
import time

from shybox.monitoring_toolkit.execution import RunMonitor


def run_application(step_seconds, fail=False):
    """Replace this function's body with your application code."""
    for step in range(1, 4):
        logging.info('Application step %s/3', step)
        time.sleep(step_seconds)
    if fail:
        raise RuntimeError('Example application failure requested with --fail.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--folder-name', type=Path,
                        default=Path(__file__).resolve().parents[2] / 'exec' / 'monitoring')
    parser.add_argument('--run-time', default=datetime.now(timezone.utc).isoformat(),
                        help='Model initialization as an ISO timestamp; default: current UTC time.')
    parser.add_argument('--ensemble', type=int, help='Optional ensemble member number.')
    parser.add_argument('--heartbeat-seconds', type=float, default=30)
    parser.add_argument('--step-seconds', type=float, default=1)
    parser.add_argument('--fail', action='store_true', help='Demonstrate a failed execution.')
    args = parser.parse_args()
    if args.step_seconds < 0:
        parser.error('--step-seconds must be nonnegative.')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    # Configure one monitor for this execution attempt/member.
    monitor = RunMonitor(
        folder_name=args.folder_name,
        run_name='nwp_icon_marche',
        application='hmc',
        run_time=args.run_time,
        run_domain='marche',
        run_mode='probabilistic' if args.ensemble is not None else 'deterministic',
        ensemble=args.ensemble,
        heartbeat_seconds=args.heartbeat_seconds,
    )

    # START: creates the JSON record and starts automatic heartbeats.
    monitor.start()
    logging.info('Monitoring record: %s', monitor.file_path)
    try:
        run_application(args.step_seconds, fail=args.fail)
    except KeyboardInterrupt as exc:
        # END: cancellation. Return the same exit code to the shell.
        monitor.finish(exit_code=130, status='cancelled', error=exc)
        logging.warning('Application interrupted.')
        return 130
    except Exception as exc:
        # END: failure. Save the error and return a nonzero exit code.
        monitor.finish(exit_code=1, error=exc)
        logging.exception('Application failed.')
        return 1
    else:
        # END: success. Also stops the heartbeat thread.
        monitor.finish(exit_code=0)
        logging.info('Application completed successfully.')
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
