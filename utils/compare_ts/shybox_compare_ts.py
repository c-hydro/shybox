#!/usr/bin/env python3

"""
APP - COMPARE TIME-SERIES

__date__ = '20260626'
__version__ = '1.0.0'
__author__ =
    'Fabio Delogu (fabio.delogu@cimafoundation.org)'
__library__ = 'shybox'

General command line:
python shybox_compare_ts.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20260625 (1.0.0) --> Beta release for shybox application
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import sys
import argparse
import time

from lib_utils_time import get_time
from lib_utils_logging import get_logger

from lib_io_json import read_settings, write_hydrograph_json

from lib_data_nc import get_data_nc, organize_data_nc
from lib_data_ascii import get_data_ascii, organize_data_ascii
from lib_data_common import merge_data, get_metadata, build_file_name
from lib_plot import plot_ts

from config_info import LOGGER_NAME, ALG_NAME, ALG_RELEASE, ALG_VERSION

# set logger
logger = logging.getLogger(LOGGER_NAME)
#-----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main
def main():

    # -----------------------------------------------------------------------------------------------------------------------
    # get algorithm args
    alg_args = get_args()
    # get algorithm settings
    alg_settings = read_settings(alg_args.settings_file)

    # get time algorithm
    try:
        alg_time = get_time(alg_args, alg_settings)
    except Exception as exc:
        print(f" ===> ERROR: parsing time: {exc}")
        sys.exit(1)

    # get type algorithm
    alg_run_type = alg_settings['type_run']

    # get logger
    get_logger(logger, alg_settings, reference_time=alg_time)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # start message
    logger.info(' ============================================================================ ')
    logger.info(' ==> ' + ALG_NAME + ' (Version: ' + ALG_VERSION + ' Release_Date: ' + ALG_RELEASE + ')')
    logger.info(' ==> START ... ')
    logger.info(' ')

    logger.info(f" ---> Settings file: {alg_args.settings_file}")
    logger.info(f" ---> Run Time: {alg_time:%Y-%m-%d %H:%M}")
    logger.info(f" ---> Run Type: {alg_run_type}")

    # Source - NetCDF
    src_nc = alg_settings.get("source", {}).get("nc", {})
    logger.info(" ---> Source (NC)")
    logger.info(f"      Folder   : {src_nc.get('folder')}")
    logger.info(f"      Filename : {src_nc.get('filename')}")

    # Source - TXT
    src_txt = alg_settings.get("source", {}).get("txt", {})
    logger.info(" ---> Source (TXT)")
    logger.info(f"      Folder   : {src_txt.get('folder')}")
    logger.info(f"      Filename : {src_txt.get('filename')}")

    # Destination - JSON
    dst_json = alg_settings.get("destination", {}).get("json", {})
    logger.info(" ---> Destination (JSON)")
    logger.info(f"      Folder   : {dst_json.get('folder')}")
    logger.info(f"      Filename : {dst_json.get('filename')}")

    # Destination - PNG
    dst_png = alg_settings.get("destination", {}).get("png", {})
    logger.info(" ---> Destination (PNG)")
    logger.info(f"      Folder   : {dst_png.get('folder')}")
    logger.info(f"      Filename : {dst_png.get('filename')}")
    logger.info(' ')

    start_time = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # get obj hydro from netcdf file
    logger.info(f" ----> Get datasets netcdf ...")
    dset_hydro_nc, metadata_hydro_nc = get_data_nc(src_nc, alg_time)
    logger.info(f" ----> Get datasets netcdf ... DONE")
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # iterate over sections
    sections_tag, sections_ascii = metadata_hydro_nc['sections_tag'], metadata_hydro_nc['sections_ascii']
    for section_id, (section_name, section_ascii) in enumerate(zip(sections_tag, sections_ascii)):

        # info section start
        logger.info(f' -----> Section {section_id}: {section_name} ... ')

        # get obj hydro txf
        logger.info(f" ------> Get datasets ascii ...")
        df_hydro_txt = get_data_ascii(src_txt, alg_time, section_ascii)

        # check hydro txt obj
        if df_hydro_txt is None:
            # info section end - skipped file not found
            logger.info(f" ------> Get datasets from ascii file ... SKIPPED")
            logger.info(f' -----> Section {section_id}: {section_name} ... SKIPPED. File not found')
            continue

        logger.info(f" ------> Get datasets ascii ... DONE")

        # organize obj hydro nc
        logger.info(f" ------> Organize datasets netcdf ... ")
        df_hydro_nc = organize_data_nc(dset_hydro_nc, section_id)
        logger.info(f" ------> Organize datasets netcdf ... DONE")

        # organize obj hydro txt
        logger.info(f" ------> Organize datasets ascii ... ")
        df_hydro_txt = organize_data_ascii(df_hydro_txt)
        logger.info(f" ------> Organize datasets ascii ... DONE")

        # organize datasets common start
        logger.info(f" ------> Organize datasets and metadata common ... ")
        # define hydro time-series
        df_hydro_common = merge_data(df_hydro_txt, df_hydro_nc)
        # define hydro metadata
        metadata_hydro_common = get_metadata(dset_hydro_nc, section_id)
        # organize datasets common end
        logger.info(f" ------> Organize datasets and metadata common ... DONE")

        # Save datasets common - start
        logger.info(f" ------> Save datasets and metadata common ... ")

        # define filename json
        file_path_json = build_file_name(dst_json, time=alg_time, tag=section_name)
        # write hydro time-series
        write_hydrograph_json(
            json_name=file_path_json,
            df_hydro_common=df_hydro_common, metadata_hydro_common=metadata_hydro_common,
            time_reference=alg_time, run_reference=alg_run_type)

        # define filename png
        file_path_png = build_file_name(dst_png, time=alg_time, tag=section_name)
        # plot hydro time-series
        plot_ts(file_name=file_path_png,
                df_hydro_common=df_hydro_common, metadata_hydro_common=metadata_hydro_common,
                time_reference=alg_time, run_reference=alg_run_type,
                time_start=None, time_end=None,
                fig_show=False)

        # Save datasets common - end
        logger.info(f" ------> Save datasets and metadata common ... DONE")

        # info section end
        logger.info(f' -----> Section {section_id}: {section_name} ... DONE')
        # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # end message
    alg_time_elapsed = round(time.time() - start_time, 1)

    logger.info(' ')
    logger.info(' ==> ' + ALG_NAME + ' (Version: ' + ALG_VERSION + ' Release_Date: ' + ALG_RELEASE + ')')
    logger.info(' ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    logger.info(' ==> ... END')
    logger.info(' ==> Bye, Bye')
    logger.info(' ============================================================================ ')
    # ------------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# helper to cli
def get_args():
    parser = argparse.ArgumentParser(
        description="Read NC hydrograph file and find related TXT files."
    )

    parser.add_argument(
        "-settings_file", "--settings_file",
        required=True,
        help="Path to JSON settings file"
    )

    parser.add_argument(
        "-time", "--time",
        default=None,
        help='Time, example: "2026-03-31 10:12"'
    )

    return parser.parse_args()
#-----------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# wrapper to main
if __name__ == "__main__":
    main()
#-----------------------------------------------------------------------------------------------------------------------
