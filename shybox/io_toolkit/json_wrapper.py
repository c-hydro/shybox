"""
Library Features:

Name:          json_wrapper
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261106'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# library
import re
import pandas as pd

from string import Formatter
from pathlib import Path

from shybox.io_toolkit.lib_io_json import (
    create_json_time_series, write_json_time_series_by_section, write_json_time_series_by_collection)

# common decoretors
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to replace selected separators in filename tags (:, !, |)
def _parse_tag(value):
    if value is None or pd.isna(value):
        raise ValueError('Missing section metadata used in an output filename.')
    tag = re.sub(r'[:!|]', '_', str(value))
    if not tag:
        raise ValueError('Section metadata cannot produce an empty filename tag.')
    return tag

# method to write time series to json hmc file
@with_logger(var_name='logger_stream')
def write_json_time_series_sections(data, file_path, *, layout='sections',
                                    sections_db=None, attrs_data=None, time=None,
                                    run_metadata=None, section_map=None, var_mapping=None,
                                    time_format='%Y-%m-%d %H:%M', run_time_format='%Y%m%d%H%M',
                                    data_no_value=-9999.0, decimal_places=None, **kwargs):

    # check method arguments (sections file or single file for all sections)
    if layout == 'single':
        layout = 'collections'
    if layout not in ('sections', 'collections'):
        raise ValueError('JSON layout must be sections or collections.')

    # check dataframe format
    if not isinstance(data, pd.DataFrame) or data.empty:
        raise ValueError('JSON section output requires a nonempty DataFrame.')
    # check dataframe structure
    if not isinstance(data.columns, pd.MultiIndex) or data.columns.nlevels != 2 or not data.columns.is_unique:
        raise ValueError('Expected unique (variable, section) DataFrame columns; ensemble data needs a separate schema.')

    # get and parse the registry
    registry = sections_db if sections_db is not None else attrs_data
    # check registry format
    if registry is None:
        registry = data.attrs.get('data')
    if not isinstance(registry, pd.DataFrame):
        logger_stream.error('Registry must be a pandas DataFrame.')
        raise TypeError('Registry must be a pandas DataFrame.')
    registry = registry.copy()

    # search tag column in registry object
    if 'tag' not in registry and registry.index.name == 'tag':
        registry = registry.reset_index()
    if 'tag' not in registry or registry['tag'].isna().any():
        logger_stream.error('Tag column is not available in registry object')
        raise ValueError('Section registry requires nonmissing tag values.')
    # parse tag column to string format (in case of numeric values)
    registry['tag'] = registry['tag'].astype(str)
    if registry['tag'].duplicated().any():
        logger_stream.error('Tag column is available in registry object but values are not unique')
        raise ValueError('Section registry tag values must be unique.')
    registry = registry.set_index('tag', drop=False)

    # get file template and placeholders
    file_template = str(file_path)
    file_placeholders = {'section', 'section_tag', 'section_id', 'section_code', 'section_name', 'basin'}

    # search file fields based on string placeholders
    file_fields = []
    for _, name, spec, conversion in Formatter().parse(file_template):
        if name is not None:
            if name not in file_placeholders or spec or conversion:
                raise ValueError(f'Unsupported JSON output placeholder: {name!r}. Resolve run/time tags first.')
            file_fields.append(name)

    # check layout to return different behaviuor in filling template
    if layout == 'sections' and not file_fields:
        raise ValueError('Per-section JSON output requires a section placeholder, for example {section_tag}.')
    if layout == 'collections' and file_fields:
        logger_stream.warning(
            f'Remove section placeholders {", ".join(dict.fromkeys(file_fields))} '
            f'from the collections filename template. Using "collections" for their values.')
        file_template = file_template.format(**{name: 'collections' for name in file_fields})
        file_fields = []

    # iterate over section(s)
    file_plans, file_outputs = [], set()
    for section in data.columns.get_level_values(1).unique():

        # get tag
        tag = str(section)
        if tag not in registry.index:
            logger_stream.warning(f'Section {tag!r} is missing from the registry.')
            continue

        # organize metadata and values of placeholder(s)
        metadata = registry.loc[tag].to_dict()
        values = {'section': tag, 'section_tag': tag, 'section_id': metadata.get('id'),
                  'section_code': metadata.get('section_code'), 'section_name': metadata.get('section_name'),
                  'basin': metadata.get('basin')}

        # define file path
        file_path = Path(file_template.format(**{name: _parse_tag(values[name]) for name in file_fields}))

        # check for layout == sections if file is already available in file_outputs
        if layout == 'sections' and file_path.resolve() in file_outputs:
            raise ValueError(f'Section output paths collide after filename sanitization: {file_path}')

        # store file outputs
        file_defined = file_path.resolve()
        file_outputs.add(file_defined)

        # add to plans the files that must be computed
        file_plans.append((section, tag, metadata, file_path))

    # create file options
    file_options = dict(time=time, run_metadata=run_metadata, section_map=section_map, var_mapping=var_mapping,
                   time_format=time_format, run_time_format=run_time_format,
                   data_no_value=data_no_value, decimal_places=decimal_places)
    # get file variables
    file_variables = data.columns.get_level_values(0).unique()

    # dump a single collection json file
    if layout == 'collections':

        # info collections start
        logger_stream.info_up(f'Write time-series JSON for collections to {file_template} ...')

        # merge data to collections
        file_records = (create_json_time_series(data.xs(section, axis=1, level=1).reindex(columns=file_variables),
                                   section_metadata=metadata, **file_options)
                   for section, _, metadata, _ in file_plans)

        # dump collections
        file_collections = write_json_time_series_by_collection(file_template, file_records)

        # info collections start
        logger_stream.info_down(f'Write time-series JSON for collections to {file_template} ... DONE')

        # return file collections
        return file_collections

    elif layout == 'sections':

        # dump section json file - iterate over section(s)
        file_manifest = {}
        for section, tag, metadata, file_path in file_plans:

            # info section start
            logger_stream.info_up(f'Write time-series JSON for section {tag} to {file_path} ...')

            # dump section time-series
            file_manifest[tag] = write_json_time_series_by_section(
                file_path, data.xs(section, axis=1, level=1).reindex(columns=file_variables),
                section_metadata=metadata, **file_options)

            # info section end
            logger_stream.info_down(f'Write time-series JSON for section {tag} to {file_path} ... DONE')

        # return file manifest
        return file_manifest
# ----------------------------------------------------------------------------------------------------------------------
