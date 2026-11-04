"""Generic local temporary storage without cached data objects."""
import json
import pickle
import tempfile
from dataclasses import dataclass
from pathlib import Path
from string import Formatter


@dataclass(frozen=True)
class DataTemporary:
    """Store arbitrary Python objects as pickle, or xarray objects as NetCDF/Zarr.

    ``file_name`` may be a plain filename or a template, e.g. ``field_{step}.nc``.
    Pass template values as keyword arguments to path/read_data/write_data.
    NetCDF/Zarr readers are lazy unless load=True; close the returned xarray
    object when finished. Pickle reads load the whole object into memory.
    Files remain available for caller-managed cleanup. Read only trusted pickles.
    """

    folder_name: str
    file_name: str = None
    writer_type: str = 'pickle'
    prefix: str = 'tmp_'

    _metadata_key = '_shybox_temporary_object'

    def __post_init__(self):

        folder = str(self.folder_name).strip() if self.folder_name is not None else ''

        if not folder or '{' in folder or '}' in folder:
            raise ValueError('Temporary folder_name must be a resolved, nonempty path.')

        suffixes = {'pickle': '.pkl', 'netcdf': '.nc', 'zarr': '.zarr'}
        if self.writer_type not in suffixes:
            raise ValueError(f'Unsupported writer_type {self.writer_type!r}; choose pickle, netcdf or zarr.')
        name = self.file_name if self.file_name is not None else 'data' + suffixes[self.writer_type]
        if not isinstance(name, str) or not name or Path(name).name != name or name in {'.', '..'}:
            raise ValueError('file_name must be a filename, without directory components.')
        fields = [field for _, field, _, _ in Formatter().parse(name) if field is not None]
        if any(not field.isidentifier() for field in fields):
            raise ValueError('Filename placeholders must be simple names, such as {step}.')

        self._check_prefix(self.prefix)

        object.__setattr__(self, 'folder_name', str(Path(folder).expanduser().resolve()))
        object.__setattr__(self, 'file_name', name)

    @staticmethod
    def _check_prefix(prefix):
        if not isinstance(prefix, str) or not prefix or '/' in prefix or '\\' in prefix:
            raise ValueError('prefix must be a nonempty directory-name prefix, without path separators.')

    def create_run(self, folder: str = None, file_name: str = None, prefix: str = None):
        """Create isolated storage, inheriting settings and the configured prefix."""

        # resolve filename and prefix
        prefix = self.prefix if prefix is None else prefix
        file_name = self.file_name if file_name is None else file_name

        # check prefix
        self._check_prefix(prefix)

        # ensure root folder exists
        root_folder = Path(self.folder_name)
        root_folder.mkdir(parents=True, exist_ok=True)

        # resolve run folder
        if folder is None:
            run_folder = Path(tempfile.mkdtemp(prefix=prefix, dir=root_folder))
        else:
            run_folder = Path(folder)
            run_folder.mkdir(parents=True, exist_ok=True)

        # create isolated storage instance
        return type(self)(folder_name=str(run_folder), file_name=file_name,
            writer_type=self.writer_type, prefix=prefix)

    def path(self, **tags):
        """Resolve arbitrary filename tags; never allow tags to escape the folder."""
        try:
            name = self.file_name.format(**tags)
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(f'Cannot resolve temporary filename {self.file_name!r}: {exc}') from exc
        if not name or Path(name).name != name or name in {'.', '..'} or '\\' in name:
            raise ValueError('Resolved file_name must not contain directory components.')
        return Path(self.folder_name) / name

    def write_data(self, data, *, tags=None, **kwargs):
        """Write one object; tags resolve filenames and kwargs configure the writer."""
        path = self.path(**(tags or {}))
        if self.writer_type == 'pickle':
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('wb') as handle:
                pickle.dump(data, handle, **({'protocol': pickle.HIGHEST_PROTOCOL} | kwargs))
            return path

        import xarray as xr
        if not isinstance(data, (xr.DataArray, xr.Dataset)):
            raise TypeError(f'{self.writer_type} temporary storage requires an xarray DataArray or Dataset.')
        if kwargs.get('compute') is False:
            raise ValueError('Temporary writes must complete immediately; compute=False is unsupported.')
        if self.writer_type == 'zarr':
            self._check_zarr()
        if isinstance(data, xr.DataArray):
            if data.name is not None and not isinstance(data.name, str):
                raise TypeError('Temporary DataArray names must be strings or None.')
            variable = data.name or '__temporary_dataarray__'
            while variable in data.coords or variable in data.dims:
                variable += '_'
            obj = data.to_dataset(name=variable)
            metadata = {'kind': 'dataarray', 'variable': variable, 'name': data.name}
        else:
            obj = data.copy(deep=False)
            metadata = {'kind': 'dataset'}
        if self._metadata_key in obj.attrs:
            raise ValueError(f'Reserved temporary metadata attribute: {self._metadata_key}')
        obj.attrs = {**obj.attrs, self._metadata_key: json.dumps(metadata)}
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.writer_type == 'netcdf':
            obj.to_netcdf(path, **({'engine': 'netcdf4'} | kwargs))
        else:
            # Format 2 supports consolidated metadata across existing readers;
            # callers can opt into format 3 with zarr_format=3.
            obj.to_zarr(path, **({'mode': 'w', 'zarr_format': 2} | kwargs))
        return path

    def read_data(self, *, tags=None, load=False, **kwargs):
        """Read one object; NetCDF/Zarr remain file-backed unless load=True."""
        path = self.path(**(tags or {}))
        if self.writer_type == 'pickle':
            with path.open('rb') as handle:
                return pickle.load(handle, **kwargs)
        import xarray as xr
        if self.writer_type == 'zarr':
            self._check_zarr()
            dataset = xr.open_zarr(path, **kwargs)
        else:
            dataset = xr.open_dataset(path, **({'engine': 'netcdf4'} | kwargs))
        try:
            metadata = json.loads(dataset.attrs.pop(self._metadata_key))
            if metadata['kind'] == 'dataarray':
                obj = dataset[metadata['variable']].rename(metadata['name'])
                obj.set_close(dataset.close)
            else:
                obj = dataset
            if load:
                obj.load()
                dataset.close()
            return obj
        except Exception:
            dataset.close()
            raise

    @staticmethod
    def _check_zarr():
        try:
            import zarr  # noqa: F401
        except ModuleNotFoundError as exc:
            raise ImportError('Zarr temporary storage requires the zarr package in the active environment.') from exc
