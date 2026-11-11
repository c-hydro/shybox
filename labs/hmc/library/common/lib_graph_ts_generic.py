"""
Library Features:

Name:          lib_graph_ts_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210113'
Version:       '1.0.0'
"""
#######################################################################################
# Libraries
import os
import pandas as pd

from library.common.lib_utils_system import make_folder

import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to configure time-series axes
def configure_ts_axes(dframe_data, time_format='%m-%d %H'):

    tick_time_period = list(dframe_data.index)
    tick_time_idx = dframe_data.index
    tick_time_labels = [tick_label.strftime(time_format) for tick_label in dframe_data.index]

    return tick_time_period, tick_time_idx, tick_time_labels
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to configure time-series attributes
def configure_ts_attrs(attrs_data,
                       tag_run_time='time_run', tag_restart_time='time_restart', tag_start_time='time_start',
                       tag_run_name='run_name', tag_run_domain='run_domain',
                       tag_section_name='section_name', tag_basin_name='section_domain',
                       tag_section_thr_alarm_discharge='section_discharge_thr_alarm',
                       tag_section_thr_alert_discharge='section_discharge_thr_alert',
                       tag_section_drainage_area='section_drained_area'):

    attrs_ts = {}
    for attr_key, attr_value in attrs_data.items():

        if attr_key == tag_run_time:
            attrs_ts[tag_run_time] = pd.Timestamp(attr_value)
        if attr_key == tag_restart_time:
            attrs_ts[tag_restart_time] = pd.Timestamp(attr_value)
        if attr_key == tag_start_time:
            attrs_ts[tag_start_time] = pd.Timestamp(attr_value)
        elif attr_key == tag_run_name:
            attrs_ts[tag_run_name] = attr_value
        elif attr_key == tag_section_name:
            attrs_ts[tag_section_name] = attr_value
        elif attr_key == tag_basin_name:
            attrs_ts[tag_basin_name] = attr_value
        elif attr_key == tag_section_thr_alarm_discharge:
            attrs_ts[tag_section_thr_alarm_discharge] = float(attr_value)
        elif attr_key == tag_section_thr_alert_discharge:
            attrs_ts[tag_section_thr_alert_discharge] = float(attr_value)
        elif attr_key == tag_section_drainage_area:
            attrs_ts[tag_section_drainage_area] = attr_value
        elif attr_key == tag_run_domain:
            attrs_ts[tag_run_domain] = attr_value

    return attrs_ts
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to plot forcing time-series
def plot_ts_forcing(file_name,
                    df_rain=None, value_min_rain=0, value_max_rain=20,
                    df_airt=None, value_min_airt=-20, value_max_airt=35,
                    df_incrad=None, value_min_incrad=-50, value_max_incrad=1200,
                    df_rh=None, value_min_rh=0, value_max_rh=100,
                    df_winds=None, value_min_winds=0, value_max_winds=20,
                    attrs_forcing=None,
                    tag_time_name='time', tag_time_units='[hour]',
                    tag_rain_name='Rain', tag_rain_units='[mm]',
                    tag_airt_name='AirT', tag_airt_units='[C]',
                    tag_incrad_name='IncRad', tag_incrad_units='[W/m^2]',
                    tag_rh_name='RH', tag_rh_units='[%]',
                    tag_winds_name='Wind', tag_winds_units='[m/s]',
                    tag_sep=' ', fig_dpi=120):

    # Configure ts attributes
    attrs_ts = configure_ts_attrs(attrs_forcing)
    # Configure ts time axes
    [tick_time_period, tick_time_idx, tick_time_labels] = configure_ts_axes(df_rain)

    # Axis labels
    label_time = tag_sep.join([tag_time_name, tag_time_units])
    label_rain = tag_sep.join([tag_rain_name, tag_rain_units])
    label_airt = tag_sep.join([tag_airt_name, tag_airt_units])
    label_incrad = tag_sep.join([tag_incrad_name, tag_incrad_units])
    label_rh = tag_sep.join([tag_rh_name, tag_rh_units])
    label_winds = tag_sep.join([tag_winds_name, tag_winds_units])

    # Open figure
    fig = plt.figure(figsize=(17, 11))
    fig.autofmt_xdate()

    # Subplot 1 [RAIN]
    ax1 = plt.subplot(5, 1, 1)
    ax1.set_xticks(tick_time_idx)
    ax1.set_xticklabels([])

    ax1.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax1.set_ylabel(label_rain, color='#000000')
    ax1.set_ylim(value_min_rain, value_max_rain)
    ax1.grid(b=True)

    p11 = ax1.bar(df_rain.index, df_rain.values[:, 0], color='#33A1C9', alpha=1, width=0.025, align='edge')
    p12 = ax1.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    legend = ax1.legend([p11[0]], [tag_rain_name], frameon=False, loc=2)
    ax1.add_artist(legend)

    ax1.set_title('Time Series \n Domain: ' + attrs_ts['run_domain'] +
                  ' \n  TypeRun: ' + attrs_ts['run_name'] +
                  ' == Time_Run: ' + str(attrs_ts['time_run']) + ' == Time_Restart: ' + str(attrs_ts['time_restart']) +
                  ' == Time_Start: ' + str(attrs_ts['time_start']))

    # Subplot 2 [AIR TEMPERATURE]
    ax2 = plt.subplot(5, 1, 2)
    ax2.set_xticks(tick_time_idx)
    ax2.set_xticklabels([])

    ax2.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax2.set_ylabel(label_airt, color='#000000')
    ax2.set_ylim(value_min_airt, value_max_airt)
    ax2.grid(b=True)

    p21 = ax2.plot(df_airt.index, df_airt.values[:, 0], color='#FF0000', linestyle='-', lw=2)
    p22 = ax2.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    legend = ax2.legend([p21[0]], [tag_airt_name], frameon=False, loc=2)
    ax2.add_artist(legend)

    # Subplot 3 [INCOMING RADIATION]
    ax3 = plt.subplot(5, 1, 3)
    ax3.set_xticks(tick_time_idx)
    ax3.set_xticklabels([])

    ax3.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax3.set_ylabel(label_incrad, color='#000000')
    ax3.set_ylim(value_min_incrad, value_max_incrad)
    ax3.grid(b=True)

    p31 = ax3.plot(df_incrad.index, df_incrad.values[:, 0], color='#9B26B6', linestyle='-', lw=2)
    p32 = ax3.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    legend = ax3.legend([p31[0]], [tag_incrad_name], frameon=False, loc=2)
    ax3.add_artist(legend)

    # Subplot 4 [RELATIVE HUMIDITY]
    ax4 = plt.subplot(5, 1, 4)
    ax4.set_xticks(tick_time_idx)
    ax4.set_xticklabels([])

    ax4.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax4.set_ylabel(label_rh, color='#000000', fontsize=6)
    ax4.set_ylim(value_min_rh, value_max_rh)
    ax4.grid(b=True)

    p41 = ax4.plot(df_rh.index, df_rh.values[:, 0], color='#0093CC', linestyle='-', lw=2)
    p42 = ax4.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    legend = ax4.legend([p41[0]], [tag_rh_name], frameon=False, loc=2)
    ax4.add_artist(legend)

    # Subplot 5 [WIND SPEED]
    ax5 = plt.subplot(5, 1, 5)
    ax5.set_xticks(tick_time_idx)
    ax5.set_xticklabels([])

    ax5.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax5.set_ylabel(label_winds, color='#000000')
    ax5.set_ylim(value_min_winds, value_max_winds)
    ax5.grid(b=True)

    p51 = ax5.plot(df_winds.index, df_winds.values[:, 0], color='#149414', linestyle='-', lw=2)
    p52 = ax5.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    legend = ax5.legend([p51[0]], [tag_winds_name], frameon=False, loc=2)
    ax5.add_artist(legend)

    ax5.set_xticks(tick_time_idx)
    ax5.set_xticklabels(tick_time_labels, rotation=90, fontsize=8)

    file_path, file_folder = os.path.split(file_name)

    if not os.path.exists(file_path):
        make_folder(file_path)
    fig.savefig(file_name, dpi=fig_dpi)

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to plot discharge time-series
def plot_ts_discharge(file_name, df_discharge_sim, attrs_discharge_sim, df_discharge_obs=None,
                      value_min_discharge=0, value_max_discharge=100,
                      df_rain=None, value_min_rain=0, value_max_rain=20,
                      df_soil_moisture=None, value_min_soil_moisture=0, value_max_soil_moisture=1,
                      tag_time_name='time', tag_time_units='[hour]',
                      tag_discharge_generic_name='Discharge',
                      tag_discharge_sim_name='Discharge Simulated',
                      tag_discharge_obs_name='Discharge Observed', tag_discharge_units='[m^3/s]',
                      tag_rain_avg_name='Rain Avg', tag_rain_accumulated_name='Rain Accumulated', tag_rain_units='[mm]',
                      tag_soil_moisture_name='Soil Moisture', tag_soil_moisture_units='[-]',
                      tag_discharge_thr_alarm='discharge thr alarm', tag_discharge_thr_alert='discharge thr alert',
                      tag_sep=' ', fig_dpi=120):

    # Configure ts attributes
    attrs_ts = configure_ts_attrs(attrs_discharge_sim)
    # Configure ts time axes
    [tick_time_period, tick_time_idx, tick_time_labels] = configure_ts_axes(df_discharge_sim)

    # Axis labels
    label_time = tag_sep.join([tag_time_name, tag_time_units])
    label_discharge_generic = tag_sep.join([tag_discharge_generic_name, tag_discharge_units])
    label_discharge_sim = tag_sep.join([tag_discharge_sim_name, tag_discharge_units])
    label_discharge_obs = tag_sep.join([tag_discharge_obs_name, tag_discharge_units])
    label_rain_avg = tag_sep.join([tag_rain_avg_name, tag_rain_units])
    label_rain_accumulated = tag_sep.join([tag_rain_accumulated_name, tag_rain_units])
    label_soil_moisture = tag_sep.join([tag_soil_moisture_name, tag_soil_moisture_units])

    # Open figure
    fig = plt.figure(figsize=(17, 11))
    fig.autofmt_xdate()

    # Subplot 1 [RAIN]
    ax1 = plt.subplot(3, 1, 1)
    ax1.set_xticks(tick_time_idx)
    ax1.set_xticklabels([])

    ax1.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax1.set_ylabel(label_rain_avg, color='#000000')
    ax1.set_ylim(value_min_rain, value_max_rain)
    ax1.grid(b=True)

    p11 = ax1.bar(df_rain.index, df_rain.values[:, 0],
                  color='#33A1C9', alpha=1, width=0.025, align='edge')

    p13 = ax1.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2)

    ax3 = ax1.twinx()
    ax3.set_ylabel(label_rain_accumulated, color='#000000')
    ax3.set_ylim(value_min_rain, value_max_rain)

    ax3.set_xticks(tick_time_idx)
    ax3.set_xticklabels([])
    ax3.set_xlim(tick_time_period[0], tick_time_period[-1])

    p31 = ax3.plot(df_rain.index, df_rain.cumsum().values[:, 0],
                   color='#33A1C9', linestyle='-', lw=1)

    # legend = ax1.legend(p11, [oRain_OBS_META['var_appearance']], frameon=False, loc=2)

    legend = ax1.legend((p11[0], p31[0]),
                        (tag_rain_avg_name, tag_rain_accumulated_name,),
                        frameon=False, loc=2)

    ax1.add_artist(legend)

    ax1.set_title('Time Series \n Section: ' + attrs_ts['section_name'] +
                  ' == Basin: ' + attrs_ts['section_domain'] +
                  ' == Area [Km^2]: ' + attrs_ts['section_drained_area'] + ' \n  TypeRun: ' + attrs_ts['run_name'] +
                  ' == Time_Run: ' + str(attrs_ts['time_run']) + ' == Time_Restart: ' + str(attrs_ts['time_restart']) +
                  ' == Time_Start: ' + str(attrs_ts['time_start']))

    # Subplot 2 [DISCHARGE]
    ax2 = plt.subplot(3, 1, (2, 3))
    p21 = ax2.plot(df_discharge_obs.index, df_discharge_obs.values[:, 0],
                   color='#000000', linestyle='--', lw=1, marker='o', ms=4)
    p22 = ax2.plot(df_discharge_sim.index, df_discharge_sim.values[:, 0],
                   color='#0000FF', linestyle='-', lw=1)

    ax2.set_xlabel(label_time, color='#000000')
    ax2.set_xlim(tick_time_period[0], tick_time_period[-1])
    ax2.set_ylabel(label_discharge_generic, color='#000000')
    ax2.set_ylim(value_min_discharge, value_max_discharge)
    ax2.grid(b=True)

    p27 = ax2.axvline(attrs_ts['time_run'], color='#000000', linestyle='--', lw=2, label='time run')
    p28 = ax2.axhline(attrs_ts['section_discharge_thr_alarm'], color='#FFA500', linestyle='--',
                      linewidth=2, label=tag_discharge_thr_alarm)
    p29 = ax2.axhline(attrs_ts['section_discharge_thr_alert'], color='#FF0000', linestyle='--',
                      linewidth=2, label=tag_discharge_thr_alert)

    ax2.set_xticks(tick_time_idx)
    ax2.set_xticklabels(tick_time_labels, rotation=90, fontsize=8)

    ax4 = ax2.twinx()
    p41 = ax4.plot(df_soil_moisture.index, df_soil_moisture.values[:, 0],
                   color='#DA70D6', linestyle='--', lw=2)

    ax4.set_ylabel(label_soil_moisture, color='#000000')
    ax4.set_ylim(value_min_soil_moisture, value_max_soil_moisture)

    ax4.set_xticks(tick_time_idx)
    ax4.set_xticklabels(tick_time_labels, rotation=90, fontsize=8)

    legend1 = ax2.legend((p21[0], p22[0], p41[0]),
                         (tag_discharge_sim_name, tag_discharge_obs_name, tag_soil_moisture_name),
                         frameon=False, ncol=2, loc=0)
    legend2 = ax2.legend((p28, p29),
                         (tag_discharge_thr_alarm, tag_discharge_thr_alert),
                         frameon=False, ncol=4, loc=9, bbox_to_anchor=(0.5, -0.2))

    ax2.add_artist(legend1)
    ax2.add_artist(legend2)

    file_path, file_folder = os.path.split(file_name)

    if not os.path.exists(file_path):
        make_folder(file_path)
    fig.savefig(file_name, dpi=fig_dpi)

    # plt.show()
    # plt.close()

# -------------------------------------------------------------------------------------
