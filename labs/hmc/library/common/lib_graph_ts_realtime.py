"""
Library Features:

Name:          lib_jupyter_plot_live
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Libraries
import os
import numpy as np
import pandas as pd
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to get information real-time
def get_info_realtime(file_dframe_live):

    index_period = file_dframe_live.index
    index_max = index_period.max()
    index_min = index_period.min()

    return index_min, index_max
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to adjust time-series real-time
def adjust_ts_realtime(file_dframe_live, file_time_idx, file_cols_n=97):

    file_rows_n = file_time_idx.__len__()
    file_data_tmp = np.zeros(shape=[file_rows_n, file_cols_n])
    file_data_tmp[:, :] = np.nan

    file_columns_tmp = list(file_dframe_live.columns)
    file_dframe_tmp = pd.DataFrame(data=file_data_tmp, index=file_time_idx, columns=file_columns_tmp)

    file_dframe_adjust = file_dframe_live.combine_first(file_dframe_tmp)

    return file_dframe_adjust
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to plot time-series real-time
def plot_ts_realtime(file_name, file_time_stamp, file_dframe,
                     fig_y_label='Discharge [m^3/s]', fig_title_generic='HMC Time-Series',
                     fig_y_min=0, fig_y_max=100, fig_dpi=150, fig_show=True, fig_jupyter=True,
                     fig_update=True):

    select_tag = list(file_dframe.columns)
    select_n = select_tag.__len__()

    file_time_string = file_time_stamp.strftime('%Y-%d-%m %H:00')

    fig_title_info = ' === Time: ' + file_time_string
    fig_title = fig_title_generic + fig_title_info

    if fig_jupyter:
        clear_output(wait=True)

    fig, ax = plt.subplots(1, 1, figsize=(15, 5))

    p = ax.plot(file_dframe.index, file_dframe[select_tag].values, linestyle='-', lw=2)

    ax.set_xlim(file_dframe.index[0], file_dframe.index[-1])
    ax.set_ylim(fig_y_min, fig_y_max)

    ax.set_title(fig_title)
    ax.set_ylabel(fig_y_label)
    ax.grid(b=True)

    p_handle_list = []
    for i in range(0, select_n):
        p_handle_list.append(p[i])
    legend = ax.legend(p_handle_list, select_tag, frameon=False, loc=2)
    ax.add_artist(legend)

    plt.show()

    if fig_update:
        if os.path.exists(file_name):
            os.remove(file_name)

    fig.savefig(file_name, dpi=fig_dpi)

    if not fig_show:
        plt.close()

# -------------------------------------------------------------------------------------
