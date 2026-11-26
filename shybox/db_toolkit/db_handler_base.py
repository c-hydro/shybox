"""
Class Features

Name:          db_handler_base
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251125'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
from collections.abc import Mapping
from netrc import NetrcParseError, netrc
from pathlib import Path

from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.db_toolkit import lib_db_backend_postgresql
from shybox.db_toolkit.lib_db_io import write_data as dispatch_write
from shybox.db_toolkit.lib_db_providers import get_provider
from shybox.logging_toolkit.logging_handler import LoggingManager

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class configuration database
class ConfigurationDB:

    # database driver(s)
    _backends = {
        'postgresql': lib_db_backend_postgresql
    }

    # init class
    def __init__(self, database: Mapping, variables: Mapping | None = None, backend: str | None = None, *,
                 output: Mapping | None = None, provider: str | Mapping = 'vda', logger: LoggingManager = None):

        # set logger
        self.logger = logger or LoggingManager(name="DB")

        self.database = dict(database)
        self._has_output_config = output is not None
        try:
            self.config_in = {} if variables is None and output is None else self.filter_variables(variables)
            self.config_out = {} if output is None else self._filter_output(variables, self.config_in, output)
        except ValueError as exc:
            self.logger.error(str(exc))
            raise

        # set backend
        self._backend = (backend if backend is not None else database.get('backend', 'postgresql')).strip().lower()

        # check if database backend is supported
        if self._backend not in self._backends:
            supported = ', '.join(sorted(self._backends))
            raise ValueError(f'Unsupported database backend {self._backend!r}. Supported backends: {supported}.')

        self._provider = get_provider(provider)

    @property
    def variables(self) -> dict:
        return self.config_in

    @property
    def outputs(self) -> dict:
        return self.config_out

    @staticmethod
    def filter_variables(variables: Mapping) -> dict:
        """Return the configured source variables that are active."""
        if not isinstance(variables, Mapping):
            raise ValueError('source.variables must be an object keyed by variable name.')

        active_variables = {}
        for name, configuration in variables.items():
            if not isinstance(configuration, Mapping):
                raise ValueError(f'source.variables.{name} must be an object.')
            active = configuration.get('active', True)
            if not isinstance(active, bool):
                raise ValueError(f'source.variables.{name}.active must be true or false.')
            if active:
                active_variables[name] = configuration

        if not active_variables:
            raise ValueError('Configure at least one active variable in source.variables.')

        return active_variables

    @staticmethod
    def _filter_output(variables: Mapping, active_variables: Mapping, destinations: Mapping) -> dict:
        if not isinstance(destinations, Mapping):
            raise ValueError('destination must be an object keyed by variable name.')

        missing = active_variables.keys() - destinations.keys()
        undeclared = destinations.keys() - variables.keys()
        errors = []
        if missing:
            errors.append(f'Active source variables missing from destination: {", ".join(sorted(missing))}.')
        if undeclared:
            errors.append(f'Destination variables missing from source.variables: {", ".join(sorted(undeclared))}.')
        if errors:
            raise ValueError(' '.join(errors))

        outputs = {}
        for name in active_variables:
            configuration = destinations[name]
            if not isinstance(configuration, Mapping):
                raise ValueError(f'destination.{name} must be an object.')
            outputs[name] = configuration
        return outputs

    @classmethod
    def filter_application(cls, application: Mapping) -> dict:
        """Validate source and destination names and keep only active pairs."""
        source = application['source']
        variables = source['variables']
        active_variables = cls.filter_variables(variables)
        return {
            **application,
            'source': {**source, 'variables': active_variables},
            'destination': cls._filter_output(variables, active_variables, application['destination']),
        }

    @property
    def backend(self) -> str:
        return self._backend

    # method to get data
    def get_data(self, var_name, time_start, time_end=None):

        # adjust time_end if define by NoneType
        if time_end is None:
            time_end = time_start

        # get variable configuration
        var_config = self.variables[var_name]
        # get variable data (based on using provider)
        var_data = self._provider.get_data(self, var_config, time_start, time_end)
        return var_data

    # method to write data
    def write_data(self, data, configuration: Mapping, *, variable: str = 'observations'):
        if self._has_output_config and variable not in self.config_out:
            raise KeyError(f'No active destination is configured for variable {variable!r}.')
        # select writer
        writer = getattr(self._provider, 'write_data', write_data_default)
        # return writer
        return writer(data, configuration, variable=variable, logger=self.logger)

    # method to connect to db
    def connect(self):
        username, password = self.get_credentials()
        return self._backends[self._backend].connect(self.database, username=username, password=password)

    # method to get database credentials (from netrc or environment)
    def get_credentials(self) -> tuple[str, str]:

        database = self.database
        method = database.get('auth_method', 'auto').lower()
        if method not in ('auto', 'netrc', 'env'):
            raise ValueError('database.auth_method must be auto, netrc or env')

        host = database.get('netrc_host', database.get('host', 'localhost'))
        if method in ('auto', 'netrc'):
            path = Path(database.get('netrc_file', '~/.netrc')).expanduser()
            try:
                if path.stat().st_mode & 0o077:
                    raise PermissionError(f'Insecure permissions on {path}; run chmod 600 {path}')
                entry = netrc(str(path)).authenticators(host)
            except (FileNotFoundError, NetrcParseError) as exc:
                if method == 'netrc':
                    raise RuntimeError(f'Cannot load .netrc credentials for {host}: {exc}') from exc
                entry = None
            if entry and entry[0] and entry[2]:
                return entry[0], entry[2]
            if method == 'netrc':
                raise RuntimeError(f'No complete .netrc credentials for {host} in {path}')

        user_env = database.get('user_env', 'SHYBOX_DB_USER')
        password_env = database.get('password_env', 'SHYBOX_DB_PASSWORD')
        username = os.environ.get(user_env) or database.get('user')
        password = os.environ.get(password_env)
        if not username or not password:
            raise RuntimeError(
                f'No database credentials available: configure ~/.netrc for {host} '
                f'or set {user_env} and {password_env}'
            )
        return username, password
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class DataDB (based on DataLocal class)
class DataDB(DataLocal):

    # private method to overwrite the DataLocal method
    def _write_data(self, data, path, **kwargs):

        # manage the file and folder
        target = Path(path).expanduser()
        if target.suffix.lower() != '.csv':
            raise ValueError('Output filename must end with .csv')
        target.parent.mkdir(parents=True, exist_ok=True)

        # info message start
        self.logger.info_up(f'Save {len(data)} records to {target} ... ')

        # dump the data
        options = self.file_args
        data.to_csv(
            target,
            sep=options.get('delimiter', ';'),
            index=False,
            encoding=options.get('encoding', 'utf-8'),
            date_format=options.get('time_format', '%Y-%m-%d %H:%M:%S'),
            na_rep=str(options.get('missing_value', '')),
            decimal=options.get('decimal', '.'),
        )

        # info message end
        self.logger.info_down(f'Save {len(data)} records to {target} ... DONE')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to write csv (default case)
def write_csv_default(
    path, data, options: Mapping, *, variable: str = 'observations', logger=None,
    column: Mapping[str, str] | None = None, **kwargs):

    # define target (folder and file_name)
    target = Path(path).expanduser()

    # use columns mapping is available
    if column is not None:
        data = data.loc[:, list(column)].rename(columns=column)

    # call the writer for db case
    writer = DataDB(
        path=str(target.parent),
        file_name=target.name,
        file_args=options,
        file_io='output',
        file_type='points_csv',
        file_format='csv',
        file_variable=variable,
        file_workflow='data_from_db',
        data_layout='points',
        logger=logger, tag=f'db_datasets_{variable}'
    )
    return writer.write_data(data)

# configure the write_data_default
def write_data_default(data, destination: Mapping, *, variable: str = 'observations', logger=None):
    """Write observation data using the shared default writer."""
    return dispatch_write(data, destination, variable=variable, logger=logger)
# ----------------------------------------------------------------------------------------------------------------------
