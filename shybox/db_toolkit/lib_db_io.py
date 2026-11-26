"""
Library Features:

Name:          lib_db_io
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261125'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pickle
from collections.abc import Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write a pickle (debug)
def write_pickle(data, path) -> Path:
    """Save a local debug snapshot to the given pickle path."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=target.parent, prefix='.data-', delete=False) as handle:
        temporary = Path(handle.name)
        try:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.close()
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    return target

# method to read a pickle (debug)
def read_pickle(path):
    with Path(path).expanduser().open('rb') as handle:
        return pickle.load(handle)
# ----------------------------------------------------------------------------------------------------------------------

def _write_default_csv(data, configuration: Mapping, *, variable: str, logger):
    from shybox.db_toolkit.db_handler_base import write_csv_default

    path = Path(configuration['path']) / configuration['file_name']
    options = configuration.get('options', configuration.get('csv', {}))
    return write_csv_default(path, data, options, variable=variable, logger=logger)


# ----------------------------------------------------------------------------------------------------------------------
# method to configure the write_data for db
def write_data(
        data, configuration: Mapping, *, writers: Mapping | None = None,
        variable: str = 'observations', logger=None, default_format: str = 'csv',):

    # if writers is defined by NoneType use the default
    if writers is None:
        writers = {'csv': _write_default_csv}

    # get file format
    file_format = configuration.get('format', configuration.get('file_format', default_format))
    # get file writer
    file_writer = writers.get(file_format.strip().lower()) if isinstance(file_format, str) else None
    if file_writer is None:
        supported = ', '.join(sorted(writers))
        raise ValueError(f'Unsupported observation output format {file_format!r}. Available formats: {supported}.')

    return file_writer(data, configuration, variable=variable, logger=logger)
# ----------------------------------------------------------------------------------------------------------------------
