"""
Library Features:

Name:          lib_db_providers
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261125'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
from abc import ABC, abstractmethod
from collections.abc import Mapping
from importlib import import_module
from inspect import signature
from pathlib import Path
from types import ModuleType

# list of PROVIDERS
_PROVIDERS = {'vda': '.access_point_db.access_point_vda'}
_LEGACY_LOCATIONS = {
    'shybox.db_toolkit.lib_db_vda.access_point': _PROVIDERS['vda'],
    'shybox.db_toolkit.lib_db_vda.access_point_vda': _PROVIDERS['vda'],
}
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class to define methods to get and write data based on provider
class DataProvider(ABC):
    """Base class for a provider with a required reader and a shared CSV writer."""

    # define get_data default method
    @abstractmethod
    def get_data(self, source, selection, start, end):
        """Read observations for the configured selection and time window."""
    # define write_data default method
    def write_data(self, data, destination, *, variable='observations', logger=None):
        """Write observations using the shared output formats."""
        from shybox.db_toolkit.db_handler_base import write_data_default

        return write_data_default(data, destination, variable=variable, logger=logger)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to try to select provider and load the module
def get_provider(name: str | Mapping = 'vda') -> ModuleType | DataProvider:
    """Load a provider from an alias, a module path, or a JSON location object.

    Provider classes derive from DataProvider and are constructed without
    arguments. Module providers can omit write_data to use the shared writer.
    """
    location = _resolve_location(name)
    module_name, separator, class_name = location.partition(':')
    module_path = _PROVIDERS.get(location.lower()) or _LEGACY_LOCATIONS.get(location)
    if module_path is None and (
        '.' not in module_name or not all(part.isidentifier() for part in module_name.split('.'))
        or (separator and not class_name.isidentifier())
    ):
        supported = ', '.join(sorted(_PROVIDERS))
        raise ValueError(
            f'Unsupported observation source.provider {name!r}. Available providers: {supported}. '
            'Use a dotted module path, module:Class, or a package/file or folder/file object.'
        )

    provider = import_module(module_path or module_name, package=__package__)
    if separator:
        provider_class = getattr(provider, class_name, None)
        if not isinstance(provider_class, type) or not issubclass(provider_class, DataProvider):
            raise TypeError(f'Data provider {name!r} must name a DataProvider subclass.')
        provider = provider_class()

    # validate get_data method
    _validate_method(
        provider, name, 'get_data', '(source, selection, start, end)',
        None, None, None, None)
    # validate write_data method
    if hasattr(provider, 'write_data'):
        _validate_method(
            provider, name, 'write_data', '(data, destination, *, variable, logger)',
            None, None, variable=None, logger=None,
        )
    return provider

# helper to to resolve location(s)
def _resolve_location(name: str | Mapping) -> str:
    if not isinstance(name, Mapping):
        return name.strip() if isinstance(name, str) else ''

    if set(name) not in ({'package', 'file'}, {'folder', 'file'}):
        raise ValueError("source.provider must contain either 'package' and 'file' or 'folder' and 'file'.")

    filename = name['file']
    if not isinstance(filename, str) or not filename.endswith('.py') or not filename[:-3].isidentifier():
        raise ValueError("source.provider.file must be a Python filename such as 'access_point_vda.py'.")
    module = filename[:-3]

    if 'package' in name:
        package = name['package']
        if not isinstance(package, str) or not all(part.isidentifier() for part in package.strip().split('.')):
            raise ValueError('source.provider.package must be a dotted Python package name.')
        return f'{package.strip()}.{module}'

    folder_name = name['folder']
    if not isinstance(folder_name, str) or not folder_name.strip():
        raise ValueError('source.provider.folder must be a filesystem path.')
    import_root = Path(__file__).resolve().parents[2]
    folder = (import_root / Path(folder_name.strip()).expanduser()).resolve()
    try:
        package = '.'.join(folder.relative_to(import_root).parts)
    except ValueError as exc:
        raise ValueError(f'source.provider.folder must be inside {import_root}. Use package/file for other packages.') from exc
    if not (folder / filename).is_file():
        raise FileNotFoundError(f'Provider file does not exist: {folder / filename}')
    return f'{package}.{module}'

# helper to validate method in provider(s)
def _validate_method(provider, provider_name, method_name, parameters, *args, **kwargs):
    method = getattr(provider, method_name, None)
    contract = f'Observation provider {provider_name!r} must expose {method_name}{parameters}.'
    if not callable(method):
        raise TypeError(contract)
    try:
        signature(method).bind(*args, **kwargs)
    except (TypeError, ValueError) as exc:
        raise TypeError(contract) from exc
# ----------------------------------------------------------------------------------------------------------------------
