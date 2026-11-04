"""Iterate analyzer sections and write Continuum JSON hydrographs."""
from pathlib import Path
import re
from string import Formatter

import pandas as pd

from shybox.io_toolkit.lib_io_json import (
    json_time_series, write_json_time_series, write_json_time_series_collection,
)
from shybox.logging_toolkit.lib_logging_utils import with_logger


def _filename_tag(value):
    if value is None or pd.isna(value):
        raise ValueError('Missing section metadata used in an output filename.')
    tag = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(value)).strip('._-')
    if not tag:
        raise ValueError('Section metadata cannot produce an empty filename tag.')
    return tag


@with_logger(var_name='logger_stream')
def write_json_time_series_sections(data, file_path, *, layout='sections',
                                    sections_db=None, attrs_data=None, time=None,
                                    run_metadata=None, section_map=None, data_map=None,
                                    time_format='%Y-%m-%d %H:%M', run_time_format='%Y%m%d%H%M',
                                    data_no_value=-9999.0, decimal_places=None):
    """Write a joined analyzer DataFrame to per-section files or one JSON array.

    Input columns are exactly (variable, section tag), as returned by
    join_time_series_by_registry. Registry matching uses tags, never row order.
    The registry defaults to data.attrs['data']; an explicit registry overrides it.
    """
    if layout not in ('sections', 'single'):
        raise ValueError('JSON layout must be sections or single.')
    if not isinstance(data, pd.DataFrame) or data.empty:
        raise ValueError('JSON section output requires a nonempty DataFrame.')
    if not isinstance(data.columns, pd.MultiIndex) or data.columns.nlevels != 2 or not data.columns.is_unique:
        raise ValueError('Expected unique (variable, section) DataFrame columns; ensemble data needs a separate schema.')
    registry = sections_db if sections_db is not None else attrs_data
    if registry is None:
        registry = data.attrs.get('data')
    if hasattr(registry, 'get_data'):
        registry = registry.get_data()
    if isinstance(registry, dict) and len(registry) == 1 and isinstance(next(iter(registry.values())), pd.DataFrame):
        registry = next(iter(registry.values()))
    if not isinstance(registry, pd.DataFrame):
        if not isinstance(registry, dict):
            raise ValueError('A section registry with unique tag values is required.')
        registry = pd.DataFrame(registry)
    registry = registry.copy()
    if 'tag' not in registry and registry.index.name == 'tag':
        registry = registry.reset_index()
    if 'tag' not in registry or registry['tag'].isna().any():
        raise ValueError('Section registry requires nonmissing tag values.')
    registry['tag'] = registry['tag'].astype(str)
    if registry['tag'].duplicated().any():
        raise ValueError('Section registry tag values must be unique.')
    registry = registry.set_index('tag', drop=False)

    template = str(file_path)
    allowed = {'section', 'section_tag', 'section_id', 'section_code', 'section_name', 'basin'}
    fields = []
    for _, name, spec, conversion in Formatter().parse(template):
        if name is not None:
            if name not in allowed or spec or conversion:
                raise ValueError(f'Unsupported JSON output placeholder: {name!r}. Resolve run/time tags first.')
            fields.append(name)
    if layout == 'sections' and not fields:
        raise ValueError('Per-section JSON output requires a section placeholder, for example {section_tag}.')
    if layout == 'single' and fields:
        raise ValueError('Single-file JSON output must not contain section placeholders.')

    plans, outputs = [], set()
    for section in data.columns.get_level_values(1).unique():
        tag = str(section)
        if tag not in registry.index:
            raise ValueError(f'Section {tag!r} is missing from the registry.')
        metadata = registry.loc[tag].to_dict()
        values = {'section': tag, 'section_tag': tag, 'section_id': metadata.get('id'),
                  'section_code': metadata.get('section_code'), 'section_name': metadata.get('section_name'),
                  'basin': metadata.get('basin')}
        path = Path(template.format(**{name: _filename_tag(values[name]) for name in fields}))
        if layout == 'sections' and path.resolve() in outputs:
            raise ValueError(f'Section output paths collide after filename sanitization: {path}')
        outputs.add(path.resolve())
        plans.append((section, tag, metadata, path))

    options = dict(time=time, run_metadata=run_metadata, section_map=section_map,
                   data_map=data_map, time_format=time_format, run_time_format=run_time_format,
                   data_no_value=data_no_value, decimal_places=decimal_places)
    variables = data.columns.get_level_values(0).unique()
    if layout == 'single':
        records = (json_time_series(data.xs(section, axis=1, level=1).reindex(columns=variables),
                                   section_metadata=metadata, **options)
                   for section, _, metadata, _ in plans)
        return write_json_time_series_collection(template, records)

    manifest = {}
    for section, tag, metadata, path in plans:
        logger_stream.info(f'Write time-series JSON for section {tag} to {path} ...')
        manifest[tag] = write_json_time_series(path, data.xs(section, axis=1, level=1).reindex(columns=variables),
                                               section_metadata=metadata, **options)
    return manifest
