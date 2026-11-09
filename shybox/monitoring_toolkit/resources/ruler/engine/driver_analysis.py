"""
Library Features:

Name:          driver_ruler_report
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20221227'
Version:       '1.0.0'
"""
#################################################################################
# Library
import logging
import time
import os
import glob
import psutil

import numpy as np
import pandas as pd

from copy import deepcopy

from .lib_info_args import logger_name
from .lib_data_io import read_obj, write_obj, write_csv, read_csv

from .lib_utils_io import filter_dframe_by_column, convert_dframe_units
from .lib_utils_system import fill_tags2string, make_folder
from .lib_utils_time import check_time_delta_limits, fill_time_delta_parts, convert_time_delta_to_seconds

from .lib_analysis_fx import get_process_info, organize_process_info
from .lib_analysis_fx import get_memory_info, organize_memory_info
from .lib_analysis_fx import get_disk_info
from .lib_analysis_fx import get_system_load_info
from .lib_analysis_fx import kill_process_tree
from .lib_analysis_plot import plot_process_info, plot_memory_info, plot_disk_info

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
#################################################################################


# -------------------------------------------------------------------------------------
# Class to manage analysis
class DriverAnalysis:

    # -------------------------------------------------------------------------------------
    # Method to initialize class
    def __init__(self, report_time, dict_algorithm, dict_process, dict_report, dict_tmp=None):

        # generic object(s)
        self.report_time = report_time
        self.dict_algorithm = dict_algorithm
        self.dict_process = dict_process
        self.dict_report = dict_report
        self.dict_tmp = dict_tmp

        # algorithm object(s)
        self.alg_flags = dict_algorithm['flags']
        self.alg_template = dict_algorithm['template']
        self.alg_tools = dict_algorithm['tools']

        # process object(s)
        self.proc_name = self.dict_process['name']
        self.proc_analysis_time_frequency = fill_time_delta_parts(self.dict_process['analysis_time_frequency'])
        self.proc_analysis_time_frequency = check_time_delta_limits(
            self.proc_analysis_time_frequency, time_delta_min='2s', time_delta_max=None)
        self.proc_analysis_seconds_frequency = convert_time_delta_to_seconds(self.proc_analysis_time_frequency)
        self.proc_analysis_time_period = fill_time_delta_parts(self.dict_process['analysis_time_period'])
        self.proc_analysis_time_period = check_time_delta_limits(
            self.proc_analysis_time_period, time_delta_min=None, time_delta_max='1h')
        self.proc_analysis_seconds_period = convert_time_delta_to_seconds(self.proc_analysis_time_period)
        self.proc_analysis_tools = self.dict_process['analysis_tools']

        # report object(s)
        self.report_file_path_ancillary = self.define_file_name(
            os.path.join(self.dict_report['ancillary']['folder_name'], self.dict_report['ancillary']['file_name']),
            report_time=self.report_time)
        self.report_file_path_destination = self.define_file_name(
            os.path.join(self.dict_report['destination']['folder_name'], self.dict_report['destination']['file_name']),
            report_time=self.report_time)
        self.report_file_path_figure = self.define_file_name(
            os.path.join(self.dict_report['figure']['folder_name'], self.dict_report['figure']['file_name']),
            report_time=self.report_time)
        self.report_file_delimiter = self.dict_report['settings']['report_delimiter']
        self.report_count_row_max = self.dict_report['settings']['report_ancillary_max_row']
        self.report_count_row_step = 0
        self.report_count_id_start = self.dict_report['settings']['report_ancillary_id_start']
        self.report_count_id_step = self.dict_report['settings']['report_ancillary_id_start']

        self.report_time_elapsed_start = time.time()
        self.report_time_elapsed_step = None

        # get process obj list
        self.proc_obj_init = get_process_info(self.proc_name, verbose=True)

        # get and organize memory info
        self.info_virtual_memory_init, self.info_swap_memory_init = get_memory_info()
        self.info_global_memory_init = organize_memory_info(self.info_virtual_memory_init, self.info_swap_memory_init)

        # get and set algorithm flag(s)
        self.flag_clean_report_file_ancillary = self.alg_flags['file']['clean_report_ancillary']
        self.flag_clean_report_file_destination = self.alg_flags['file']['clean_report_destination']
        self.flag_clean_report_file_figure = self.alg_flags['file']['clean_report_figure']
        self.flag_activate_report_alg_organize = self.alg_flags['algorithm']['activate_report_organize']
        self.flag_activate_report_alg_dump = self.alg_flags['algorithm']['activate_report_dump']
        self.flag_activate_report_alg_view = self.alg_flags['algorithm']['activate_report_view']
        self.flag_activate_report_alg_kill = self.alg_flags['algorithm']['activate_report_kill']

        # set dframe tag(s)
        self.report_index_tag = 'time'

        # flag to clean ancillary file(s) previously saved
        if self.flag_clean_report_file_ancillary:
            if self.flag_activate_report_alg_organize:
                report_file_path_ancillary_generic = self.define_file_name(
                    self.report_file_path_ancillary, report_time=self.report_time, report_id='*')

                report_file_path_ancillary_list = glob.glob(report_file_path_ancillary_generic)
                for report_file_path_ancillary_step in report_file_path_ancillary_list:
                    if os.path.exists(report_file_path_ancillary_step):
                        os.remove(report_file_path_ancillary_step)

        # flag to clean destination file(s) previously saved
        if self.flag_clean_report_file_destination:
            if self.flag_activate_report_alg_dump:
                report_file_path_destination_generic = self.report_file_path_destination
                if os.path.exists(report_file_path_destination_generic):
                    os.remove(report_file_path_destination_generic)

        # flag to clean figure file(s) previously saved
        if self.flag_clean_report_file_figure:
            if self.flag_activate_report_alg_view:
                report_file_path_figure_generic = self.define_file_name(
                    self.report_file_path_figure, report_time=self.report_time, report_type='*')

                report_file_path_figure_list = glob.glob(report_file_path_figure_generic)
                for report_file_path_figure_step in report_file_path_figure_list:
                    if os.path.exists(report_file_path_figure_step):
                        os.remove(report_file_path_figure_step)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define file name
    def define_file_name(self, file_name_raw, report_time=None, report_type=None, report_id=None):

        report_time_stamp = None
        if report_time is not None:
            report_time_stamp = pd.Timestamp(report_time)

        if report_id is not None:
            if isinstance(report_id, (int, float)):
                report_id = str(int(report_id))
            elif isinstance(report_id, str):
                pass
            else:
                log_stream.error(' ===> Report id format is not supported')
                raise NotImplemented('Case not implemented yet')

        alg_template_keys = self.alg_template
        alg_template_values = {
            'process_name': self.proc_name,
            'report_type': report_type,
            "report_id": report_id,
            'report_sub_path': report_time_stamp,
            'report_datetime': report_time_stamp
        }

        file_name_def = fill_tags2string(file_name_raw, alg_template_keys, alg_template_values)[0]

        return file_name_def

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define time execution
    def define_clock_execution(self, clock_stamp_start_in=None, clock_stamp_end_in=None, clock_update=True):

        if clock_update:
            clock_stamp_start_out = pd.Timestamp.now(tz='UTC').round(freq='s')
            if self.proc_analysis_time_period is not None:
                clock_stamp_end_out = clock_stamp_start_out + pd.Timedelta(self.proc_analysis_time_period)
                clock_update = False
            else:
                clock_stamp_end_out = clock_stamp_start_out + pd.Timedelta(self.proc_analysis_time_frequency)
                clock_update = True
        else:
            clock_stamp_start_out = deepcopy(clock_stamp_start_in + pd.Timedelta(self.proc_analysis_time_frequency))
            clock_stamp_end_out = deepcopy(clock_stamp_end_in)

        return clock_stamp_start_out, clock_stamp_end_out, clock_update
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define tool attributes
    @staticmethod
    def define_tool_attributes(tool_name, tool_collections):
        tool_attrs = None
        if tool_name in list(tool_collections.keys()):
            tool_attrs = tool_collections[tool_name]
        return tool_attrs
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to execute report analysis
    def execute_report_analysis(self):

        info_process_collections, info_memory_collections = {}, {}
        info_disk_collections, info_system_load_collection = {}, {}
        if 'tool_info_process' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_process', self.alg_tools)

            # get and organize process info collections
            info_process_raw = get_process_info(self.proc_name, **tool_attrs)
            info_process_collections = organize_process_info(info_process_raw, **tool_attrs)

        if 'tool_info_memory' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_memory', self.alg_tools)
            # get and organize memory info collections
            info_virtual_memory, info_swap_memory = get_memory_info()
            info_memory_collections = organize_memory_info(info_virtual_memory, info_swap_memory, **tool_attrs)

        if 'tool_info_disk' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_disk', self.alg_tools)
            # get and organize disk info collections
            info_disk_collections = get_disk_info(**tool_attrs)

        if 'tool_info_system_load' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_system_load', self.alg_tools)
            # get and organize system load info collections
            info_system_load_collection = get_system_load_info(**tool_attrs)

        # merge information
        info_report_collections = {**info_process_collections, **info_memory_collections,
                                   **info_disk_collections, **info_system_load_collection}

        # time elapsed
        self.report_time_elapsed_step = round(time.time() - self.report_time_elapsed_start, 1)

        # set empty dictionary to NoneTye
        if not info_report_collections:
            info_report_collections = None
        else:
            info_report_collections['time_elapsed'] = self.report_time_elapsed_step

        return info_report_collections
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to freeze report analysis
    def freeze_report_analysis(self, analysis_time, analysis_collections):

        # get and update file path ancillary
        report_file_path_anc = deepcopy(self.report_file_path_ancillary)
        report_file_path_anc = self.define_file_name(report_file_path_anc, report_id=self.report_count_id_step)

        if not os.path.exists(report_file_path_anc):

            # create ancillary folder
            report_folder_name_anc, report_file_name_anc = os.path.split(report_file_path_anc)
            make_folder(report_folder_name_anc)

            # create analysis dframe
            analysis_dframe = pd.DataFrame(data=analysis_collections, index=[analysis_time])
            analysis_dframe.index.name = self.report_index_tag

            # save analysis dframe
            write_obj(report_file_path_anc, analysis_dframe)

        else:

            # read analysis dframe previously saved
            analysis_dframe_tmp = read_obj(report_file_path_anc)
            # delete old ancillary file
            if os.path.exists(report_file_path_anc):
                os.remove(report_file_path_anc)

            # create analysis dframe
            analysis_dframe_update = pd.DataFrame(data=analysis_collections, index=[analysis_time])
            # merge analysis dframe
            analysis_dframe = pd.concat([analysis_dframe_tmp, analysis_dframe_update], axis=0)
            analysis_dframe.index.name = self.report_index_tag

            # save analysis dframe
            write_obj(report_file_path_anc, analysis_dframe)

        # update counter(s)
        self.report_count_row_step += 1
        if self.report_count_row_step == self.report_count_row_max:
            self.report_count_row_step = 0
            self.report_count_id_step += 1

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to view report analysis
    def view_report_analysis(self, report_analysis_src, report_process_n=0):

        if 'tool_info_process' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_process', self.alg_tools)
            tool_attrs['prefix_name'] = tool_attrs['prefix_name'].format(proc_n=report_process_n)

            # filter analysis by column (prefix or name)
            report_analysis_filter = filter_dframe_by_column(report_analysis_src, **tool_attrs)
            # define report analysis file
            report_analysis_file = self.report_file_path_figure.format(report_type=tool_attrs['prefix_name'])

            # plot and save figure
            if report_analysis_filter is not None:
                report_analysis_view = convert_dframe_units(report_analysis_filter, dframe_units='G', **tool_attrs)
                plot_process_info(report_analysis_view, dframe_file_path=report_analysis_file,
                                  prefix_name=tool_attrs['prefix_name'])

        if 'tool_info_memory' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_memory', self.alg_tools)
            tool_attrs['prefix_name'] = tool_attrs['prefix_name_virtual_memory']

            # filter analysis by column (prefix or name)
            report_analysis_filter = filter_dframe_by_column(report_analysis_src, **tool_attrs)
            # define report analysis file
            report_analysis_file = self.report_file_path_figure.format(report_type=tool_attrs['prefix_name'])

            # plot and save figure
            if report_analysis_filter is not None:
                report_analysis_view = convert_dframe_units(
                    report_analysis_filter, dframe_units='G', dframe_format='{:0.3}', **tool_attrs)
                plot_memory_info(report_analysis_view, dframe_file_path=report_analysis_file)

        if 'tool_info_disk' in self.proc_analysis_tools:

            # get tool attributes
            tool_attrs = self.define_tool_attributes('tool_info_disk', self.alg_tools)

            # filter analysis by column (prefix or name)
            report_analysis_filter = filter_dframe_by_column(report_analysis_src, **tool_attrs)
            # define report analysis file
            report_analysis_file = self.report_file_path_figure.format(report_type=tool_attrs['prefix_name'])

            # plot and save figure
            if report_analysis_filter is not None:
                report_analysis_view = convert_dframe_units(
                    report_analysis_filter, dframe_units='T', dframe_format='{:0.5}', **tool_attrs)
                plot_disk_info(report_analysis_view, dframe_file_path=report_analysis_file)



    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize machine analysis
    def organize(self):

        # info report start
        log_stream.info(' ----> Organize report ... ')

        # flag to activate organize part
        if self.flag_activate_report_alg_organize:

            # define clock execution
            clock_stamp_start_out, clock_stamp_end_out, clock_update_out = self.define_clock_execution(
                clock_stamp_start_in=None, clock_stamp_end_in=None, clock_update=True)

            while True:

                # define clock time step
                clock_time_step = deepcopy(clock_stamp_start_out)
                # info clock time step start
                log_stream.info(' -----> Time "' + str(clock_time_step) + '" ... ')
                log_stream.info(' ------> Period: "' + str(clock_stamp_start_out) + '" :: "' + str(clock_stamp_end_out) + '"')

                # check analysis time period
                log_stream.info(' ------> Analysis time ... ')
                if clock_time_step > clock_stamp_end_out:
                    log_stream.info(' ------> Analysis time ... EXPIRED. EXIT')
                    break
                else:
                    log_stream.info(' ------> Analysis time ... CONTINUE')

                # get report information
                info_report = self.execute_report_analysis()

                # check analysis report
                log_stream.info(' ------> Analysis information ... ')
                if info_report is None:
                    log_stream.info(' ------> Analysis information ... EMPTY. EXIT')
                    break
                else:
                    log_stream.info(' ------> Analysis information  ... CONTINUE')

                # save report information
                self.freeze_report_analysis(clock_time_step, info_report)

                # compute the time information
                clock_stamp_start_in = deepcopy(clock_stamp_start_out)
                clock_stamp_end_in = deepcopy(clock_stamp_end_out)
                clock_update_in = deepcopy(clock_update_out)

                # define clock execution (in while cycles)
                clock_stamp_start_out, clock_stamp_end_out, clock_update_out = self.define_clock_execution(
                    clock_stamp_start_in=clock_stamp_start_in, clock_stamp_end_in=clock_stamp_end_in,
                    clock_update=clock_update_in)

                # compute time sleep (if defined)
                log_stream.info(' ------> Sleep time ... ')
                time_loop = time.time()
                if self.proc_analysis_seconds_frequency is not None:
                    # time.sleep(self.proc_analysis_seconds_frequency)
                    time_sleep = round(self.proc_analysis_seconds_frequency - ((time.time() - time_loop) % 60.0))
                    log_stream.info(' ------> Sleep time ... WAIT FOR "' + str(time_sleep) + ' SECONDS"')
                    time.sleep(time_sleep)
                else:
                    log_stream.info(' ------> Sleep time ... NOT SET')

                # info clock time step start
                log_stream.info(' -----> Time "' + str(clock_time_step) + '" ... DONE')

            # info report end
            log_stream.info(' ----> Organize analysis report ... DONE')
        else:
            # info report end
            log_stream.info(' ----> Organize analysis report ... NOT ACTIVATED')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to dump analysis report
    def dump(self):

        # info report start
        log_stream.info(' ----> Dump analysis report ... ')

        # flag to activate dump part
        if self.flag_activate_report_alg_dump:

            # get file ancillary and destination
            report_file_path_anc_tmp = deepcopy(self.report_file_path_ancillary)
            report_file_path_dst = deepcopy(self.report_file_path_destination)

            # get start, end and list counter(s)
            report_count_id_start = self.report_count_id_start
            report_count_id_end = self.report_count_id_step
            report_count_ids = np.arange(report_count_id_start, report_count_id_end + 1, 1).tolist()

            # flag to clean destination file previously saved
            if self.flag_clean_report_file_destination:
                if os.path.exists(report_file_path_dst):
                    os.remove(report_file_path_dst)

            if not os.path.exists(report_file_path_dst):
                report_analysis_collections = None
                for report_count_id in report_count_ids:

                    # define report ancillary analysis file
                    report_file_path_anc_id = self.define_file_name(report_file_path_anc_tmp, report_id=report_count_id)
                    # get report ancillary analysis file
                    log_stream.info(' -----> Get analysis file "' + report_file_path_anc_id + '" ... ')
                    # read ancillary analysis file
                    if os.path.exists(report_file_path_anc_id):

                        # read ancillary report file
                        report_analysis_id = read_obj(report_file_path_anc_id)

                        # merge ancillary file
                        if report_analysis_collections is None:
                            report_analysis_collections = deepcopy(report_analysis_id)
                        else:
                            # merge analysis dframe
                            report_analysis_collections = pd.concat(
                                [report_analysis_collections, report_analysis_id], axis=0)

                        log_stream.info(' -----> Get analysis file "' + report_file_path_anc_id + '" ... DONE')
                    else:
                        log_stream.warning(' ===> File not found')
                        log_stream.info(' -----> Get analysis file "' + report_file_path_anc_id + '" ... SKIPPED')

                # dump analysis to csv file
                log_stream.info(' -----> Dump analysis file "' + report_file_path_dst + '" ... ')

                report_folder_name_dst, report_file_name_dst = os.path.split(report_file_path_dst)
                make_folder(report_folder_name_dst)

                write_csv(report_file_path_dst, report_analysis_collections, file_separator=self.report_file_delimiter)
                log_stream.info(' -----> Dump analysis file "' + report_file_path_dst + '" ... DONE')

                # info report end
                log_stream.info(' ----> Dump analysis report ... DONE')
            else:
                # info report end
                log_stream.info(' ----> Dump analysis report ... PREVIOUSLY SAVED')
        else:
            # info report end
            log_stream.info(' ----> Dump analysis report ... NOT ACTIVATED')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to view analysis
    def view(self):

        # info report start
        log_stream.info(' ----> View analysis report ... ')

        # flag to activate view part
        if self.flag_activate_report_alg_view:

            # get file destination and figure
            report_file_path_dst = deepcopy(self.report_file_path_destination)
            report_file_path_figure = deepcopy(self.report_file_path_figure)

            # flag to clean figure file previously saved
            if self.flag_clean_report_file_figure:
                if os.path.exists(report_file_path_figure):
                    os.remove(report_file_path_figure)

            if os.path.exists(report_file_path_dst):

                # read analysis from csv file
                report_file_delimiter = self.report_file_delimiter
                report_analysis = read_csv(report_file_path_dst, file_separator=report_file_delimiter) #self.report_file_delimiter)

                # plot analysis
                self.view_report_analysis(report_analysis)

                # info report start
                log_stream.info(' ----> View analysis report ... DONE')
            else:
                log_stream.warning(' ===> File "' + report_file_path_dst + '" not found')
                log_stream.info(' ----> View analysis report ... SKIPPED')
        else:
            # info report start
            log_stream.info(' ----> View analysis report ... NOT ACTIVATED')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to kill analysis
    def kill(self):

        # info report start
        log_stream.info(' ----> Kill analysis report ... ')

        if self.flag_activate_report_alg_kill:

            proc_name_user, proc_obj_list = self.proc_name, self.proc_obj_init

            if proc_obj_list is not None:
                for proc_obj_step in proc_obj_list:

                    proc_pid_step = proc_obj_step.pid

                    try:
                        proc_name_step, proc_status_step = proc_obj_step.name(), proc_obj_step.status()
                        proc_gone_step, proc_alive_step = kill_process_tree(proc_pid_step)

                        if proc_name_step != proc_name_user:
                            log_stream.warning(
                                ' ===> ProcessName set in memory "' + proc_name_step +
                                '" is not equal to ProcessName set by user "' + proc_name_user + '"')

                        log_stream.info(' -----> Kill ::: ProcessName "' + proc_name_user +
                                        '" ::: ProcessID: "' + str(proc_pid_step) + '" ::: ProcessStatus: "' +
                                        proc_status_step + '" ::: KILLED')

                    except psutil.NoSuchProcess:

                        log_stream.info(
                            ' -----> Kill ::: ProcessName "' + proc_name_user + '" ::: ProcessID: "' + str(proc_pid_step) +
                            '" ::: ProcessStatus: "terminated" ::: NOSUCHPROCESS')

                log_stream.info(' ----> Kill analysis report ... DONE')

            else:
                log_stream.info(' ----> Kill analysis report ... SKIPPED. PROCESS LIST IS NOT DEFINED')
        else:
            log_stream.info(' ----> Kill analysis report ... NOT ACTIVATED')
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
