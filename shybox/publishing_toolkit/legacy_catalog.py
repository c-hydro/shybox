"""Read HAT PNG/JSON pairs without changing the legacy archive."""
from datetime import datetime
import hashlib
import json
from pathlib import Path


def legacy_time(value):
    if not value:
        return None
    value = str(value)
    formats = {10: '%Y%m%d%H', 12: '%Y%m%d%H%M', 16: '%Y-%m-%d %H:%M'}
    fmt = formats.get(len(value))
    if fmt:
        try:
            return datetime.strptime(value, fmt).isoformat() + 'Z'
        except ValueError:
            pass
    raise ValueError(f'Unsupported HAT timestamp: {value}')


class LegacyFigureCatalog:
    def __init__(self, folder=None, *, enabled=True):
        self.folder = Path(folder).resolve() if folder else None
        self.enabled = enabled and self.folder is not None

    def discover(self):
        products, issues = [], []
        if not self.enabled or not self.folder.is_dir():
            return {'products': products, 'issues': issues}
        for kind in ('maps', 'time_series'):
            for path in sorted((self.folder / kind).rglob('*.json')):
                image = path.with_suffix('.png')
                if not path.resolve().is_relative_to(self.folder) or not image.resolve().is_relative_to(self.folder):
                    continue
                try:
                    if not image.is_file():
                        raise ValueError('Matching PNG is missing.')
                    metadata = json.loads(path.read_text(encoding='utf-8'))
                    if not isinstance(metadata, dict):
                        raise ValueError('Figure metadata must be an object.')
                    product = self._product(path, kind, metadata)
                    products.append(product)
                except (OSError, ValueError, TypeError, KeyError, AttributeError, IndexError) as exc:
                    issues.append(f'{path.relative_to(self.folder)}: {exc}')
        products.sort(key=lambda item: (item['publication_time'], item['title']), reverse=True)
        return {'products': products, 'issues': issues}

    def _product(self, path, kind, metadata):
        parts = path.relative_to(self.folder).parts
        if (kind == 'maps' and len(parts) != 4) or (kind == 'time_series' and len(parts) != 5):
            raise ValueError('Expected HAT collection/publication-time/figure layout.')
        publication = legacy_time(path.parent.name)
        group = parts[1] if kind == 'maps' else parts[2]
        base = {'id': hashlib.sha256(path.relative_to(self.folder).as_posix().encode()).hexdigest()[:24],
                'kind': kind, 'group': group, 'publication_time': publication,
                'date': publication[:10], 'basin': None, 'section': None, 'window': None,
                'units': None, 'time_from': None, 'time_to': None, 'sources': [],
                'metadata_file': path.relative_to(self.folder).as_posix(),
                'image_file': path.with_suffix('.png').relative_to(self.folder).as_posix()}
        if kind == 'maps':
            data = metadata['data_attributes']
            analysis = metadata['analysis_attributes']
            graph = metadata.get('graph_attributes', {})
            base.update(title=graph.get('var_description', group), variable=data.get('var_name_data'),
                        method=analysis.get('var_mode'), window=analysis.get('var_temporal_window'),
                        direction=analysis.get('var_direction_window'), units=data.get('units'),
                        time_from=legacy_time(data.get('time_from')), time_to=legacy_time(data.get('time_to')))
        else:
            sources = [{**value, 'label': key.removeprefix('file_attributes_') if key != 'file_attributes' else 'reference'}
                       for key, value in metadata.items() if key.startswith('file_attributes') and isinstance(value, dict)]
            if not sources:
                raise ValueError('HAT time-series source attributes are missing.')
            primary = next((value for value in sources if value['label'] == 'nwp'), sources[0])
            base.update(basin=primary.get('section_domain', parts[1]), section=primary.get('section_name'),
                        section_code=primary.get('section_code'), variable='discharge', method='time_series',
                        title=primary.get('section_name', path.stem), units='m³/s')
            base['sources'] = [{'label': value['label'], 'run_name': value.get('run_name'),
                                'run_time': legacy_time(value.get('time_run')),
                                'time_start': legacy_time(value.get('time_start')),
                                'time_restart': legacy_time(value.get('time_restart'))} for value in sources]
        return base

    def resolve(self, product_id, asset='image'):
        key = {'image': 'image_file', 'metadata': 'metadata_file'}.get(asset)
        if key is None:
            raise KeyError('Unknown figure asset.')
        for product in self.discover()['products']:
            if product['id'] == product_id:
                path = (self.folder / product[key]).resolve()
                if path.is_relative_to(self.folder) and path.is_file():
                    return path
        raise KeyError('Published figure not found.')
