"""
Library Features:

Name:          lib_analysis_plot
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20221227'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import os
import pandas as pd

from .lib_utils_system import make_folder
from .lib_utils_io import convert_dframe_string2num
from .lib_info_args import logger_name

import matplotlib.pylab as plt

# Set debug level for matplotlib font manager
logging.getLogger('matplotlib.font_manager').disabled = True

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to plot disk information
def plot_disk_info(dframe_analysis, dframe_file_path=None, columns_name=None,
                   prefix_name='disk_usage', prefix_separator='_',
                   fig_y_label='disk', fig_x_label='time',
                   fig_y_unit_1='T', fig_y_unit_2='%', fig_x_unit='seconds',
                   fig_y_lim_min_1=0, fig_y_lim_max_1=2,
                   fig_title=None, fig_dpi=150, **kwargs):

    if prefix_name is None:
        prefix_name = 'disk_usage'
    if prefix_separator is None:
        prefix_separator = '_'
    if columns_name is None:
        columns_name = ['total', 'used', 'free', 'percent']

    columns_amount_tag, columns_percent_tag = [], []
    for columns_step in columns_name:
        columns_tmp = prefix_separator.join([prefix_name, columns_step])
        if columns_tmp in list(dframe_analysis.columns):
            if 'percent' in columns_tmp:
                columns_percent_tag.append(columns_tmp)
            else:
                columns_amount_tag.append(columns_tmp)

    dframe_amount_mixed = dframe_analysis.loc[:, columns_amount_tag]
    dframe_percent = dframe_analysis.loc[:, columns_percent_tag]

    dframe_amount_num = convert_dframe_string2num(dframe_amount_mixed)

    time_start, time_end = dframe_amount_num.index[0], dframe_amount_num.index[-1]

    if fig_title is None:
        fig_title = 'disk usage \n' \
                    '== time start: "' + time_start + '" ::: time end: "' + time_end + '" == '

    # plot figure
    fig, (top, bottom) = plt.subplots(nrows=2, figsize=(10, 6))
    top.set_title(fig_title, fontsize=10, fontweight="bold")

    # get plot dataframe 1
    ax1 = dframe_amount_num.plot(ax=top, lw=1, colormap='jet', marker='.', markersize=2) # title=fig_title)
    # get plot dataframe 2
    ax2 = dframe_percent.plot(ax=bottom, lw=1, colormap='jet', marker='.', markersize=2, use_index=False)

    # set axis and label 1
    # ax1.set_xticks(x_ticks, minor=True)
    # ax1.set_xticks([], minor=True)
    ax1.grid('on', which='minor', axis='x')
    ax1.grid('on', which='major', axis='x')
    ax1.grid('on', which='minor', axis='y')
    ax1.grid('on', which='major', axis='y')
    # ax1.set_xlabel(x_label + ' [' + x_unit + ']')
    ax1.set_ylabel(fig_y_label + ' [' + fig_y_unit_1 + ']', fontsize=8)
    ax1.set_ylim([fig_y_lim_min_1, fig_y_lim_max_1])
    ax1.tick_params(axis='both', labelsize=6)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax1.get_yticklabels(), visible=True)
    ax1.legend(fontsize=6)

    # set axis and label 2
    #ax2.set_xticks(x_ticks, minor=True)
    ax2.grid('on', which='minor', axis='x')
    ax2.grid('on', which='major', axis='x')
    ax2.grid('on', which='minor', axis='y')
    ax2.grid('on', which='major', axis='y')
    ax2.set_xlabel(fig_x_label + ' [' + fig_x_unit + ']', fontsize=8)
    ax2.set_ylabel(fig_y_label + ' [' + fig_y_unit_2 + ']', fontsize=8)
    ax2.set_ylim([0, 100])
    ax2.tick_params(axis='both', labelsize=6)
    plt.setp(ax2.get_xticklabels(), visible=True)
    plt.setp(ax2.get_yticklabels(), visible=True)
    ax2.legend(fontsize=6)

    # set ticks
    x_start = 0
    x_med = round((len(dframe_amount_num) - 1) / 2)
    x_end = len(dframe_amount_num) - 1
    x_list = [x_start, x_med, x_end]

    # x_ticks = pd.date_range(start=dframe_amount_num.index.min(), end=dframe_amount_num.index.max(), freq=fig_x_unit)
    top.xaxis.label.set_visible(False)
    top.set_xticks(x_list)
    top.set_xticks([], minor=True)
    bottom.xaxis.set_visible(True)
    bottom.set_xticks(x_list)
    bottom.set_xticks([], minor=True)
    date_ticks = []
    for date_select in dframe_amount_num.index[x_list]:
        date_ticks.append(date_select)
    bottom.set_xticklabels(date_ticks)

    # save figure
    if dframe_file_path is not None:

        dframe_file_folder, dframe_file_name = os.path.split(dframe_file_path)
        if not os.path.exists(dframe_file_folder):
            make_folder(dframe_file_folder)
        fig.savefig(dframe_file_path, dpi=fig_dpi)
        plt.close(fig)

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to plot process information
def plot_process_info(dframe_analysis, dframe_file_path=None, columns_name=None,
                      prefix_name='process_memory', prefix_separator='_',
                      fig_y_label='process', fig_x_label='time',
                      fig_y_unit_1='G', fig_y_unit_2='%', fig_x_unit='seconds',
                      fig_y_lim_min_1=0, fig_y_lim_max_1=2,
                      fig_title=None, fig_dpi=150, **kwargs):

    if prefix_name is None:
        prefix_name = 'process_memory'
    if prefix_separator is None:
        prefix_separator = '_'
    if columns_name is None:
        columns_name = ['rss', 'vms', 'uss', 'shared', 'swap', 'data',
                        'percent_rss', 'percent_vms', 'percent_uss', 'percent_shared', 'percent_swap', 'percent_data']

    columns_amount_tag, columns_percent_tag = [], []
    for columns_step in columns_name:
        columns_tmp = prefix_separator.join([prefix_name, columns_step])
        if columns_tmp in list(dframe_analysis.columns):
            if 'percent' in columns_tmp:
                columns_percent_tag.append(columns_tmp)
            else:
                columns_amount_tag.append(columns_tmp)

    dframe_amount_mixed = dframe_analysis.loc[:, columns_amount_tag]
    dframe_percent = dframe_analysis.loc[:, columns_percent_tag]

    dframe_amount_num = convert_dframe_string2num(dframe_amount_mixed)

    time_start, time_end = dframe_amount_num.index[0], dframe_amount_num.index[-1]

    if fig_title is None:
        fig_title = 'process memory \n' \
                    '== time start: "' + time_start + '" ::: time end: "' + time_end + '" == '

    # plot figure
    fig, (top, bottom) = plt.subplots(nrows=2, figsize=(10, 6))
    top.set_title(fig_title, fontsize=10, fontweight="bold")

    # get plot dataframe 1
    ax1 = dframe_amount_num.plot(ax=top, lw=2, colormap='jet', marker='.', markersize=5) # title=fig_title)
    # get plot dataframe 2
    ax2 = dframe_percent.plot(ax=bottom, lw=2, colormap='jet', marker='.', markersize=5, use_index=False)

    # set axis and label 1
    # ax1.set_xticks(x_ticks, minor=True)
    # ax1.set_xticks([], minor=True)
    ax1.grid('on', which='minor', axis='x')
    ax1.grid('on', which='major', axis='x')
    ax1.grid('on', which='minor', axis='y')
    ax1.grid('on', which='major', axis='y')
    # ax1.set_xlabel(x_label + ' [' + x_unit + ']')
    ax1.set_ylabel(fig_y_label + ' [' + fig_y_unit_1 + ']', fontsize=8)
    ax1.set_ylim([fig_y_lim_min_1, fig_y_lim_max_1])
    ax1.tick_params(axis='both', labelsize=6)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax1.get_yticklabels(), visible=True)
    ax1.legend(fontsize=6)

    # set axis and label 2
    #ax2.set_xticks(x_ticks, minor=True)
    ax2.grid('on', which='minor', axis='x')
    ax2.grid('on', which='major', axis='x')
    ax2.grid('on', which='minor', axis='y')
    ax2.grid('on', which='major', axis='y')
    ax2.set_xlabel(fig_x_label + ' [' + fig_x_unit + ']', fontsize=8)
    ax2.set_ylabel(fig_y_label + ' [' + fig_y_unit_2 + ']', fontsize=8)
    ax2.set_ylim([0, 100])
    ax2.tick_params(axis='both', labelsize=6)
    plt.setp(ax2.get_xticklabels(), visible=True)
    plt.setp(ax2.get_yticklabels(), visible=True)
    ax2.legend(fontsize=6)

    # set ticks
    x_start = 0
    x_med = round((len(dframe_amount_num) - 1) / 2)
    x_end = len(dframe_amount_num) - 1
    x_list = [x_start, x_med, x_end]

    # x_ticks = pd.date_range(start=dframe_amount_num.index.min(), end=dframe_amount_num.index.max(), freq=fig_x_unit)
    top.xaxis.label.set_visible(False)
    top.set_xticks(x_list)
    top.set_xticks([], minor=True)
    bottom.xaxis.set_visible(True)
    bottom.set_xticks(x_list)
    bottom.set_xticks([], minor=True)
    date_ticks = []
    for date_select in dframe_amount_num.index[x_list]:
        date_ticks.append(date_select)
    bottom.set_xticklabels(date_ticks)

    # save figure
    if dframe_file_path is not None:

        dframe_file_folder, dframe_file_name = os.path.split(dframe_file_path)
        if not os.path.exists(dframe_file_folder):
            make_folder(dframe_file_folder)
        fig.savefig(dframe_file_path, dpi=fig_dpi)
        plt.close(fig)

    pass
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to plot memory information
def plot_memory_info(dframe_analysis, dframe_file_path=None, columns_name=None,
                     prefix_name='virtual_memory', prefix_separator='_',
                     fig_y_label='memory', fig_x_label='time',
                     fig_y_unit_1='G', fig_y_unit_2='%', fig_x_unit='seconds',
                     fig_y_lim_min_1=0, fig_y_lim_max_1=100,
                     fig_title=None, fig_dpi=150, **kwargs):

    if prefix_name is None:
        prefix_name = 'virtual_memory'
    if prefix_separator is None:
        prefix_separator = '_'
    if columns_name is None:
        columns_name = ['total', 'used', 'free', 'buffers', 'cached', 'available', 'percent']

    columns_amount_tag, columns_percent_tag = [], []
    for columns_step in columns_name:
        columns_tmp = prefix_separator.join([prefix_name, columns_step])
        if columns_tmp in list(dframe_analysis.columns):
            if 'percent' in columns_tmp:
                columns_percent_tag.append(columns_tmp)
            else:
                columns_amount_tag.append(columns_tmp)

    dframe_amount_mixed = dframe_analysis.loc[:, columns_amount_tag]
    dframe_percent = dframe_analysis.loc[:, columns_percent_tag]

    dframe_amount_num = convert_dframe_string2num(dframe_amount_mixed)

    time_start, time_end = dframe_amount_num.index[0], dframe_amount_num.index[-1]

    if fig_title is None:
        fig_title = 'memory analysis \n' \
                    '== time start: "' + time_start + '" ::: time end: "' + time_end + '" == '

    # plot figure
    fig, (top, bottom) = plt.subplots(nrows=2, figsize=(10, 6))
    top.set_title(fig_title, fontsize=10, fontweight="bold")

    # get plot dataframe 1
    ax1 = dframe_amount_num.plot(ax=top, lw=2, colormap='jet', marker='.', markersize=5) # title=fig_title)
    # get plot dataframe 2
    ax2 = dframe_percent.plot(ax=bottom, lw=2, colormap='jet', marker='.', markersize=5, use_index=False)

    # set axis and label 1
    # ax1.set_xticks(x_ticks, minor=True)
    # ax1.set_xticks([], minor=True)
    ax1.grid('on', which='minor', axis='x')
    ax1.grid('on', which='major', axis='x')
    ax1.grid('on', which='minor', axis='y')
    ax1.grid('on', which='major', axis='y')
    # ax1.set_xlabel(x_label + ' [' + x_unit + ']')
    ax1.set_ylabel(fig_y_label + ' [' + fig_y_unit_1 + ']', fontsize=8)
    ax1.set_ylim([0, 100])
    ax1.tick_params(axis='both', labelsize=6)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax1.get_yticklabels(), visible=True)
    ax1.legend(fontsize=6)

    # set axis and label 2
    #ax2.set_xticks(x_ticks, minor=True)
    ax2.grid('on', which='minor', axis='x')
    ax2.grid('on', which='major', axis='x')
    ax2.grid('on', which='minor', axis='y')
    ax2.grid('on', which='major', axis='y')
    ax2.set_xlabel(fig_x_label + ' [' + fig_x_unit + ']', fontsize=8)
    ax2.set_ylabel(fig_y_label + ' [' + fig_y_unit_2 + ']', fontsize=8)
    ax2.set_ylim([fig_y_lim_min_1, fig_y_lim_max_1])
    ax2.tick_params(axis='both', labelsize=6)
    plt.setp(ax2.get_xticklabels(), visible=True)
    plt.setp(ax2.get_yticklabels(), visible=True)
    ax2.legend(fontsize=6)

    # set ticks
    x_start = 0
    x_med = round((len(dframe_amount_num) - 1) / 2)
    x_end = len(dframe_amount_num) - 1
    x_list = [x_start, x_med, x_end]

    # x_ticks = pd.date_range(start=dframe_amount_num.index.min(), end=dframe_amount_num.index.max(), freq=fig_x_unit)
    top.xaxis.label.set_visible(False)
    top.set_xticks(x_list)
    top.set_xticks([], minor=True)
    bottom.xaxis.set_visible(True)
    bottom.set_xticks(x_list)
    bottom.set_xticks([], minor=True)
    date_ticks = []
    for date_select in dframe_amount_num.index[x_list]:
        date_ticks.append(date_select)
    bottom.set_xticklabels(date_ticks)

    # save figure
    if dframe_file_path is not None:

        dframe_file_folder, dframe_file_name = os.path.split(dframe_file_path)
        if not os.path.exists(dframe_file_folder):
            make_folder(dframe_file_folder)
        fig.savefig(dframe_file_path, dpi=fig_dpi)
        plt.close(fig)

# -------------------------------------------------------------------------------------
