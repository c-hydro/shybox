"""Check source grids against the terrain grid expected by Continuum/S3M."""
import numpy as np
import xarray as xr


def read_geographical_info(data, x=None, y=None):
    if isinstance(data, xr.DataArray):
        data = data.to_dataset(name='__reference')
    if not isinstance(data, xr.Dataset):
        raise TypeError('Geographical information requires an xarray Dataset or DataArray.')
    def find(name, candidates):
        name = name or next((key for key in candidates if key in data.variables), None)
        if name is None or name not in data:
            raise ValueError('Missing geographical coordinates; configure x/y names explicitly.')
        coordinate = data[name]
        unit = coordinate.attrs.get('units')
        if unit and unit not in ('degrees_east', 'degrees_north', 'degree_east', 'degree_north', 'degrees', 'degree'):
            raise ValueError('Geographical coordinates must use degrees, not projected units.')
        return coordinate
    lon = find(x, ('longitude', 'Longitude', 'lon', 'X'))
    lat = find(y, ('latitude', 'Latitude', 'lat', 'Y'))
    if lon.ndim == lat.ndim == 1:
        dims = (lat.dims[0], lon.dims[0])
        xx, yy = np.meshgrid(lon.values, lat.values)
    elif lon.ndim == lat.ndim == 2 and lon.dims == lat.dims:
        dims, xx, yy = lon.dims, lon.values, lat.values
    else:
        raise ValueError('Coordinates must be 1D axes or matching 2D longitude/latitude arrays.')
    if (len(set(dims)) != 2 or xx.size == 0 or not np.isfinite(xx).all() or not np.isfinite(yy).all()
            or np.any(abs(xx) > 180) or np.any(abs(yy) > 90)):
        raise ValueError('Expected finite geographical longitude/latitude coordinates in degrees.')
    epsg = data['crs'].attrs.get('epsg_code') if 'crs' in data else data.attrs.get('epsg_code')
    if epsg is not None and str(epsg).upper().replace('EPSG:', '') != '4326':
        raise ValueError('Model forcing coordinates must use geographic EPSG:4326.')
    for key, expected in (('nrows', xx.shape[0]), ('ncols', xx.shape[1])):
        if key in data.attrs and int(data.attrs[key]) != expected:
            raise ValueError(f'Grid attribute {key} disagrees with geographical array dimensions.')
    return {'dims': dims, 'x': xx, 'y': yy, 'shape': xx.shape,
            'bounds': (float(xx.min()), float(yy.min()), float(xx.max()), float(yy.max()))}


def check_geographical_info(data_source, data_expected, source_x=None, source_y=None, atol=1e-6):
    """Return compatibility and index ordering; never silently interpolate."""
    source = read_geographical_info(data_source, source_x, source_y)
    expected = read_geographical_info(data_expected)
    result = {'same_grid': False, 'requires_interpolation': True,
              'source': source, 'expected': expected, 'y_index': None, 'x_index': None}
    if source['shape'] == expected['shape']:
        ny, nx = source['shape']
        for yi in (np.arange(ny), np.arange(ny)[::-1]):
            for xi in (np.arange(nx), np.arange(nx)[::-1]):
                if (np.allclose(source['x'][np.ix_(yi, xi)], expected['x'], atol=atol, rtol=0)
                        and np.allclose(source['y'][np.ix_(yi, xi)], expected['y'], atol=atol, rtol=0)):
                    result.update(same_grid=True, requires_interpolation=False, y_index=yi, x_index=xi)
                    return result
    return result
