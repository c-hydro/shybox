"""Shared monitoring JSON, timestamp and configuration utilities."""
from .io import read_status, write_status, utc_now, time_string
from .configuration import resolve_monitoring_config
