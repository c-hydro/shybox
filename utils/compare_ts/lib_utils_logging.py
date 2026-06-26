"""
Library Features:

Name:           lib_utils_logging
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260421'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import sys

from datetime import datetime

from lib_utils_time import resolve_time_tags
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get logger
def get_logger(logger, settings, reference_time=None):
    log_settings = settings.get("logging", {})

    level_str = log_settings.get("level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    log_folder_template = log_settings.get("folder", "./log")
    log_filename = log_settings.get("filename", "cell2img_h122.log")
    rotate_daily = log_settings.get("rotate_daily", False)

    if reference_time is None:
        reference_time = datetime.now()

    log_folder = resolve_time_tags(
        log_folder_template,
        {
            "time_step": reference_time,
            "time_start": reference_time,
            "time_end": reference_time,
            "time_run": reference_time
        }
    )

    os.makedirs(log_folder, exist_ok=True)

    if rotate_daily:
        timestamp = reference_time.strftime("%Y%m%d")
        log_filename = f"{timestamp}_{log_filename}"

    log_path = os.path.join(log_folder, log_filename)

    logger.handlers = []
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
# ----------------------------------------------------------------------------------------------------------------------
