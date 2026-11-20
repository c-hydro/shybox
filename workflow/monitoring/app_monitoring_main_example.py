#!/usr/bin/python3
"""
SHYBOX - Snow HYdro toolBOX - Application lifecycle monitoring demo

__date__ = '20261119'
__version__ = '1.4.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org),
     Andrea Libertino (andrea.libertino@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python -m workflow.monitoring.app_monitoring_main_example -settings_file workflow/monitoring/app_monitoring.example_run.json
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import logging
import time
from decimal import Decimal

from shybox.logging_toolkit.logging_handler import LoggingManager
from shybox.config_toolkit.arguments_handler import ArgumentsManager
from shybox.config_toolkit.config_handler import ConfigManager
from shybox.monitoring_toolkit.monitoring_execution_handler import MonitoringExecutionManager
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# algorithm information
project_name = 'shybox'
alg_name = 'Workflow for monitoring a demo application'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-11-20'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# example of application
def run_application(step_seconds=1, total_seconds=10, fail=False):

    interval, duration  = Decimal(str(step_seconds)), Decimal(str(total_seconds))
    if not interval.is_finite() or interval <= 0:
        raise ValueError('step_seconds must be finite and positive.')
    if not duration.is_finite() or duration < 0:
        raise ValueError('total_seconds must be finite and nonnegative.')

    # checks
    full_steps, remainder = divmod(duration, interval)
    step_count = int(full_steps) + bool(remainder)
    # iterations
    for step in range(1, step_count + 1):
        logging.info('Application step %s/%s', step, step_count)
        delay = remainder if remainder and step == step_count else interval
        time.sleep(float(delay))

    # simulate a fail in the application
    if fail:
        raise RuntimeError('Example application failure requested.')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# script main
def main(view_table: bool = False, argv=None):

    # ------------------------------------------------------------------------------------------------------------------
    ## CONFIGURATION MANAGEMENT
    # get file settings
    alg_args_obj = ArgumentsManager(handlers=['stream'], settings_folder=os.path.dirname(os.path.realpath(__file__)))
    alg_args_file, alg_args_time = alg_args_obj.get()

    # get configuration settings
    alg_cfg_obj = ConfigManager.from_source(src=str(alg_args_file), root_key="configuration",
                                           application_key=None, convert_none_to_nan=False)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## MONITORING MANAGEMENT
    alg_app_monitoring = alg_cfg_obj.get_section("monitoring", raise_if_missing=True,
                                                 warning_not_filled_by_lut=False)
    if alg_args_time is not None:
        alg_app_monitoring['run_time'] = alg_args_time
    # view monitoring section
    alg_cfg_obj.view(section=alg_app_monitoring, table_name='application [cfg application monitoring]', table_print=view_table)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## LOGGING MANAGEMENT
    # get application logging
    alg_app_log = alg_cfg_obj.get_section("log", root_key=None)
    # define application logging (by default some information are not defined)
    alg_app_log = alg_cfg_obj.fill_section_by_default(
        alg_app_log, {'file_name': 'app_monitoring_main_example.log'}, fill_method='method-by_tmp',)

    # view application logging
    alg_cfg_obj.view(section=alg_app_log, table_name='application [cfg application logging]',
                     table_print=view_table)

    # set logging instance
    LoggingManager.setup(
        logger_folder=alg_app_log['path'], logger_file=alg_app_log['file_name'],
        logger_format="%(asctime)s %(name)-15s %(levelname)-8s %(message)-80s %(filename)-20s:[%(lineno)-6s - %(funcName)-20s()]",
        handlers=alg_app_log.get('handlers', ['stream']),
        force_reconfigure=True,
        arrow_base_len=3, arrow_prefix='-', arrow_suffix='>',
        warning_dynamic=False, error_dynamic=False, warning_fixed_prefix="===> ", error_fixed_prefix="===> ",
        level=10
    )

    # define logging instance
    logging_handle = LoggingManager(
        name="shybox_algorithm_monitoring_example",
        level=logging.INFO, use_arrows=True, arrow_dynamic=True, arrow_tag="algorithm",
        set_as_current=True)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    ## INFO START
    # info algorithm (start)
    logging_handle.info_header(LoggingManager.rule_line("=", 78))
    logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    logging_handle.info_header('START ... ', blank_after=True)

    # time algorithm
    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # instantiate monitoring
    app_cfg_monitor = MonitoringExecutionManager(**alg_app_monitoring)

    try:
        with app_cfg_monitor:
            logging_handle.info('Monitoring record: %s', app_cfg_monitor.file_path)
            run_application(
                step_seconds=alg_app_monitoring.get('step_seconds', 1),
                total_seconds=alg_app_monitoring.get('total_seconds', 4),
                fail=alg_app_monitoring.get('fail', False),
            )
    except KeyboardInterrupt:
        logging_handle.warning('Application interrupted.')
        return 130
    except Exception:
        logging_handle.exception('Application failed.')
        return 1
    else:
        logging_handle.info('Application completed successfully.')

        ## INFO END
        # info algorithm (end)
        alg_time_elapsed = round(time.time() - start_time, 1)

        logging_handle.info_header(alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')',
                                   blank_before=True)
        logging_handle.info_header('TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
        logging_handle.info_header('... END')
        logging_handle.info_header('Bye, Bye')
        logging_handle.info_header(LoggingManager.rule_line("=", 78))

        return 0
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == '__main__':
    raise SystemExit(main(view_table=True))
# ----------------------------------------------------------------------------------------------------------------------
