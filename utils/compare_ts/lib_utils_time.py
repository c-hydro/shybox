"""
Library Features:

Name:           lib_utils_time
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
from datetime import datetime, timedelta
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to parse time
def parse_time(time_value):
    accepted_formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d%H%M",
        "%Y%m%d%H"
    ]

    for fmt in accepted_formats:
        try:
            dt = datetime.strptime(time_value, fmt)
            return dt.replace(minute=0, second=0, microsecond=0)
        except ValueError:
            pass

    raise ValueError(f"Unsupported time format: {time_value}")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to get time
def get_time(args, settings):
    time_value = args.time or settings.get("time")

    if time_value is None:
        raise RuntimeError(
            "Time is not defined. Use --time or define 'time' in the JSON."
        )

    return parse_time(time_value)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to resolve time tags
def resolve_time_tags(template_string, time_dict):
    template_resolved = str(template_string)

    for time_key, time_value in time_dict.items():
        if time_value is None:
            continue

        tag_pattern = r"\{" + re.escape(time_key) + r":([^}]+)\}"

        def replace_tag(match):
            time_format = match.group(1)
            return time_value.strftime(time_format)

        template_resolved = re.sub(tag_pattern, replace_tag, template_resolved)

    return template_resolved
# ----------------------------------------------------------------------------------------------------------------------
