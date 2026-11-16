"""Store shared forcing and publish member copies with selected NetCDF updates."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile


def file_identity(file_name):
    """Include upstream file changes in the deterministic cache signature."""
    path = Path(file_name).expanduser().resolve()
    stat = path.stat()
    return {'path': str(path), 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns,
            'ctime_ns': stat.st_ctime_ns}


def file_digest(file_name):
    digest = hashlib.sha256()
    with open(file_name, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def local_source(file_name, folder):
    """Expand compressed sources privately; never write beside upstream inputs."""
    source = Path(file_name).resolve()
    if source.suffix != '.gz':
        return source
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    destination = folder / (hashlib.sha256(str(source).encode()).hexdigest()[:16] + '.nc')
    if not destination.exists():
        with gzip.open(source, 'rb') as stream, destination.open('wb') as output:
            shutil.copyfileobj(stream, output)
    return destination


class DatasetModeStore:
    """Persistent uncompressed deterministic files, validated before reuse.

    A context includes source identities, geography and processing settings.
    Interrupted writes never become reusable entries. No live dataset objects
    are stored and each entry has a content hash. Processing is sequential.
    """

    def __init__(self, folder, *, reuse=True):
        self.folder = Path(folder).expanduser().resolve()
        self.folder.mkdir(parents=True, exist_ok=True)
        self.reuse = reuse

    def prepare(self, context, variables, build):
        """Return (cached path, reused); build(path) writes a complete plain NC."""
        payload = json.dumps({'schema_version': 1, 'context': context}, sort_keys=True, allow_nan=False)
        key = hashlib.sha256(payload.encode()).hexdigest()
        output = self.folder / (key + '.nc')
        manifest = self.folder / (key + '.json')
        if self.reuse and output.is_file() and manifest.is_file():
            try:
                record = json.loads(manifest.read_text())
                if record['context'] == payload and record['sha256'] == file_digest(output):
                    self._validate(output, variables)
                    return output, True
            except (ValueError, KeyError, OSError):
                pass
        with tempfile.TemporaryDirectory(prefix='build-', dir=self.folder) as folder:
            temporary = Path(folder) / 'forcing.nc'
            build(temporary)
            self._validate(temporary, variables)
            record = {'context': payload, 'sha256': file_digest(temporary)}
            temporary_manifest = Path(folder) / 'manifest.json'
            temporary_manifest.write_text(json.dumps(record, indent=2) + '\n')
            os.replace(temporary, output)
            os.replace(temporary_manifest, manifest)
        return output, False

    @staticmethod
    def _validate(path, variables):
        from netCDF4 import Dataset
        with Dataset(path) as dataset:
            missing = set(variables) - set(dataset.variables)
            if missing:
                raise ValueError(f'{path}: processed forcing is missing variables {sorted(missing)}.')


def update_netcdf(file_name, update_file, variables):
    """Update only named fields in a private member copy, preserving shared data."""
    import numpy as np
    from netCDF4 import Dataset

    with Dataset(update_file) as source, Dataset(file_name, 'a') as destination:
        missing = set(variables) - set(source.variables)
        if missing:
            raise ValueError(f'Probabilistic update is missing variables {sorted(missing)}.')
        for name, dimension in source.dimensions.items():
            if name not in destination.dimensions or len(destination.dimensions[name]) != len(dimension):
                raise ValueError(f'Incompatible update dimension {name!r}.')
        for name in ('longitude', 'latitude', 'time'):
            if name in source.variables and name in destination.variables:
                left, right = source[name], destination[name]
                if (left.dimensions != right.dimensions or
                        not np.array_equal(left[:], right[:], equal_nan=True) or
                        any(getattr(left, key, None) != getattr(right, key, None)
                            for key in ('units', 'calendar'))):
                    raise ValueError(f'Incompatible update coordinate {name!r}.')
        for name in variables:
            value = source[name]
            value.set_auto_maskandscale(False)
            if name in destination.variables:
                target = destination[name]
                if target.dimensions != value.dimensions or target.dtype != value.dtype:
                    raise ValueError(f'Incompatible update variable {name!r}.')
            else:
                options = {}
                if '_FillValue' in value.ncattrs():
                    options['fill_value'] = value.getncattr('_FillValue')
                target = destination.createVariable(name, value.datatype, value.dimensions, zlib=True, **options)
            target.setncatts({key: value.getncattr(key) for key in value.ncattrs() if key != '_FillValue'})
            target.set_auto_maskandscale(False)
            target[:] = value[:]


def publish_member(base_file, updates, file_name, *, compress=True):
    """Copy shared forcing, add member fields and atomically publish the result.

    ``updates`` contains (plain NetCDF path, output variable names) pairs.
    The base may be None when every variable is probabilistic. Neither base
    nor update files is modified. A failed merge leaves the old output intact.
    """
    updates = list(updates)
    destination = Path(file_name).expanduser().resolve()
    plain = destination.with_suffix('') if destination.suffix == '.gz' else destination
    destination = Path(str(plain) + '.gz') if compress else plain
    protected = {Path(path).resolve() for path, _ in updates}
    if base_file is not None:
        protected.add(Path(base_file).resolve())
    if destination in protected or plain in protected:
        raise ValueError('A member destination cannot overwrite a cached input.')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.member-', dir=destination.parent) as folder:
        staging = Path(folder) / 'forcing.nc'
        if base_file is not None:
            shutil.copyfile(base_file, staging)
        elif updates:
            first, variables = updates.pop(0)
            DatasetModeStore._validate(first, variables)
            shutil.copyfile(first, staging)
        else:
            raise ValueError('A member requires deterministic or probabilistic forcing.')
        for update, variables in updates:
            update_netcdf(staging, update, variables)
        if compress:
            zipped = Path(folder) / 'forcing.nc.gz'
            with staging.open('rb') as source, gzip.open(zipped, 'wb') as output:
                shutil.copyfileobj(source, output)
            os.replace(zipped, destination)
        else:
            os.replace(staging, destination)
    counterpart = plain if compress else Path(str(plain) + '.gz')
    if counterpart.exists():
        counterpart.unlink()
    return str(destination)
