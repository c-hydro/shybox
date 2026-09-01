
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import pandas as pd
import xarray as xr

from typing import Union

from shybox.orchestrator_toolkit.lib_orchestrator_utils_processes_upd import as_process
from shybox.dataset_toolkit.dataset_handler_local import DataLocal
from shybox.logging_toolkit.lib_logging_utils import with_logger

from shybox.processing_ts_toolkit.lib_proc_compute import compute_average_over_mask
from shybox.io_toolkit.lib_io_tiff import read_tiff_base
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to execute average over mask
@as_process(input_type='pandas', adapter_type={'data': 'list_to_darray'}, output_type='pandas')
@with_logger(var_name='logger_stream')
def exec_average_over_mask(
        data: xr.DataArray, sections_db: Union[pd.DataFrame, None],
        mask: Union[DataLocal, xr.DataArray, None],
        default_name: str = "data", ref_column: str = 'tag', ref_delimiter: Union[str, None] = '.',
        time_dim: str = "time",
        **kwargs):

    # info method start
    logger_stream.info_up('Execute average over mask ... ')

    # check data format
    if not isinstance(data, xr.DataArray):
        logger_stream.error('Object data is defined by xr.DataArray. Return None')
        raise TypeError('Object data is not DataArray. Got {}'.format(type(data)))
    # get data variable name
    data_name = data.name or default_name

    # if mask is defined by NoneType, nothing to do
    if mask is None:
        logger_stream.warning('Object mask is defined by NoneType. Return None')
        return None

    # single DataArray mask
    if isinstance(mask, xr.DataArray):

        # compute data frame
        df_out = compute_average_over_mask(data=data, mask=mask, var_name=data_name, **kwargs)
        return df_out

    # one mask for each section
    elif isinstance(mask, DataLocal):

        # get file mask pattern
        file_mask_pattern = mask.loc_pattern

        # check section db
        if sections_db is None:
            logger_stream.error("Object 'section_db' is defined by NoneType. Exit")
            raise ValueError("Object 'sections_db' must be defined when mask is DataLocal.")

        # iterate over sections
        df_collection = []
        for section_idx, section_row in sections_db.iterrows():

            # get sections fields
            section_fields = section_row.to_dict()

            # keep original tag for results
            section_tag = section_fields.get(ref_column,f"section_{section_idx}")

            # info section start
            logger_stream.info_up(f'Section {section_tag} ... ')

            # modify temporary tag for file/path substitution
            if ref_delimiter is not None:
                section_fields[ref_column] = str(section_tag).replace(":",ref_delimiter)

            # fill mask path using sections fields
            file_mask = file_mask_pattern.format(**section_fields)

            # read mask
            mask_da = read_tiff_base(file_mask,var_name="mask")

            # compute values
            data_df = compute_average_over_mask(data=data, mask=mask_da, var_name=section_tag,**kwargs)

            # convert section dataframe to series
            if time_dim in data_df.columns:
                section_series = data_df.set_index(time_dim)[section_tag]
            else:
                section_series = data_df[section_tag]
            # set series name
            section_series.name = section_tag

            # collect section
            df_collection.append(section_series)

            # info section end
            logger_stream.info_down(f'Section {section_tag} ... DONE')

        # check dataframe collection
        if not df_collection:
            # info method end (done)
            logger_stream.info_down('Execute average over mask ... FAILED. All datasets is null')
            return None

        # concatenate ALL sections at once
        df_out = pd.concat(df_collection,axis=1)
        # defragment dataframe
        df_out = df_out.copy()
        # restore time column
        df_out = df_out.rename_axis(time_dim).reset_index()
        # set dataframe name
        df_out.name = data_name

        # info method end (done)
        logger_stream.info_down('Execute average over mask ... DONE')

        return df_out

    else:

        # info method end (failed)
        logger_stream.info_down('Execute average over mask ... FAILED. Mask is not correctly defined')
        raise TypeError(f"'mask' must be DataLocal, xr.DataArray or None. Got {type(mask)}")
# ----------------------------------------------------------------------------------------------------------------------
