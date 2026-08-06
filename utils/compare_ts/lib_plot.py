"""
Library Features:

Name:           lib_plot
Author(s):      Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:           '20260626'
Version:        '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
import pandas as pd
import numpy as np
import matplotlib.pylab as plt

# Font sizes
title_fontsize = 16
subtitle_fontsize = 13
axis_label_fontsize = 12
tick_fontsize = 10
legend_fontsize = 10
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to plot time-series
def plot_ts(
        file_name,
        df_hydro_common, metadata_hydro_common,
        time_reference, run_reference='NA',
        time_start=None, time_end=None,
        fig_show=False
):

    df_hydro_common = df_hydro_common.copy()
    df_hydro_common["time"] = pd.to_datetime(df_hydro_common["time"])
    time_reference = pd.to_datetime(time_reference)

    n_rows = len(df_hydro_common)

    has_scenario_min = "scenario_min" in df_hydro_common.columns
    has_scenario_max = "scenario_max" in df_hydro_common.columns
    has_scenario_mean = "scenario_mean" in df_hydro_common.columns

    if time_start is None:
        time_start = df_hydro_common["time"].min()
    else:
        time_start = pd.to_datetime(time_start)

    if time_end is None:
        time_end = df_hydro_common["time"].max()
    else:
        time_end = pd.to_datetime(time_end)

    if time_start >= time_end:
        raise RuntimeError(
            f"'time_start' ({time_start}) must be before 'time_end' ({time_end})."
        )

    if not (time_start <= time_reference <= time_end):
        raise RuntimeError(
            f"'time_reference' ({time_reference}) is outside the plotting period "
            f"[{time_start}, {time_end}]."
        )

    df_hydro_common = df_hydro_common.loc[
        (df_hydro_common["time"] >= time_start) &
        (df_hydro_common["time"] <= time_end)
    ].reset_index(drop=True)

    if df_hydro_common.empty:
        raise RuntimeError(
            f"No hydrograph data available in period [{time_start}, {time_end}]."
        )

    df_before = df_hydro_common[df_hydro_common["time"] <= time_reference]
    df_after = df_hydro_common[df_hydro_common["time"] > time_reference]

    section = metadata_hydro_common.get("section_name", "NA")
    catchment = metadata_hydro_common.get("catchment_name", "NA")
    tag = metadata_hydro_common.get("tag", "NA")
    station = metadata_hydro_common.get("station_name", "NA")
    domain = metadata_hydro_common.get("domain_name", "NA")
    thrs_1 = metadata_hydro_common.get("threshold_level_1", -9999)
    thrs_2 = metadata_hydro_common.get("threshold_level_2", -9999)
    thrs_3 = metadata_hydro_common.get("threshold_level_3", -9999)

    fig, ax = plt.subplots(figsize=(12, 5))

    legend_handles = {}
    legend_labels = {}

    if has_scenario_min and has_scenario_max:
        h = ax.fill_between(
            df_hydro_common["time"],
            df_hydro_common["scenario_min"],
            df_hydro_common["scenario_max"],
            color="steelblue",
            alpha=0.30,
            label="prob_scenario_range"
        )
        legend_handles["prob_scenario_range"] = h
        legend_labels["prob_scenario_range"] = "prob_scenario_range"

    if has_scenario_mean:
        h, = ax.plot(
            df_hydro_common["time"],
            df_hydro_common["scenario_mean"],
            color="steelblue",
            linewidth=1,
            label="prob_scenario_mean"
        )
        legend_handles["prob_scenario_mean"] = h
        legend_labels["prob_scenario_mean"] = "prob_scenario_mean"

    if "simulated_discharge" in df_hydro_common.columns:
        h, = ax.plot(
            df_hydro_common["time"],
            df_hydro_common["simulated_discharge"],
            color="tomato",
            linewidth=1,
            label="deterministic_ts"
        )
        legend_handles["deterministic_ts"] = h
        legend_labels["deterministic_ts"] = "deterministic_ts"

    if "observed_discharge" in df_hydro_common.columns:
        h, = ax.plot(
            df_before["time"],
            df_before["observed_discharge"],
            color="black",
            linewidth=1,
            linestyle="-",
            label="observed_ts"
        )
        legend_handles["observed_ts"] = h
        legend_labels["observed_ts"] = "observed_ts"

        ax.plot(
            df_after["time"],
            df_after["observed_discharge"],
            color="black",
            linewidth=1,
            linestyle=":",
            label="_nolegend_"
        )

    # thresholds without legend
    for value, color in [
        (thrs_1, "yellow"),
        (thrs_2, "orange"),
        (thrs_3, "red"),
    ]:
        if value is not None and not pd.isna(value) and float(value) != -9999:
            ax.axhline(
                y=float(value),
                color=color,
                linewidth=1,
                linestyle="-",
                label="_nolegend_"
            )

    # reference time without legend
    ax.axvline(
        time_reference,
        color="black",
        linewidth=1,
        linestyle="--",
        label="_nolegend_"
    )

    # vertical tag near reference line
    ax.text(
        time_reference,
        0.98,
        f"{time_reference:%Y-%m-%d %H:%M}",
        transform=ax.get_xaxis_transform(),
        rotation=90,
        va="top",
        ha="right",
        fontsize=6,
        color="black"
    )

    # y limit
    valid_values = []
    for col in [
        "scenario_min",
        "scenario_max",
        "scenario_mean",
        "observed_discharge",
        "simulated_discharge"
    ]:
        if col in df_hydro_common.columns:
            values = pd.to_numeric(df_hydro_common[col], errors="coerce")
            values = values[np.isfinite(values)]
            if not values.empty:
                valid_values.append(values.max())

    real_ymax = max(valid_values) if valid_values else 0

    if thrs_3 is not None and not pd.isna(thrs_3) and float(thrs_3) != -9999:
        thrs_3_value = float(thrs_3)

        if real_ymax > thrs_3_value:
            y_max = real_ymax
        else:
            y_max = max(
                thrs_3_value * 1.10,
                thrs_3_value + 25
            )
    else:
        y_max = real_ymax * 1.10 if real_ymax > 0 else 1

    ax.set_ylim(0, y_max)

    # x limit
    ax.set_xlim(time_start, time_end)

    # Main title
    fig.suptitle(
        f"Section: {section} - Catchment: {catchment}",
        fontsize=10,
        fontweight="bold",
        y=0.975
    )

    # Subtitle
    fig.text(
        0.5,
        0.94,
        (
            f"Type: {run_reference} | Station: {station} | Domain: {domain} | Tag: {tag}\n"
            f"Reference: {time_reference:%Y-%m-%d %H:%M} | "
            f"Period: {time_start:%Y-%m-%d %H:%M} -> {time_end:%Y-%m-%d %H:%M}"
        ),
        ha="center",
        va="top",
        fontsize=8
    )
    ax.set_xlabel("Time", fontsize=6, labelpad=2)
    ax.set_ylabel("Discharge [m3/s]", fontsize=6)
    ax.tick_params(axis="x", labelsize=6)
    ax.tick_params(axis="y", labelsize=6)

    ax.grid(True)

    legend_order = [
        "prob_scenario_range",
        "prob_scenario_mean",
        "deterministic_ts",
        "observed_ts"
    ]

    handles = [legend_handles[k] for k in legend_order if k in legend_handles]
    labels = [legend_labels[k] for k in legend_order if k in legend_labels]

    ax.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.20),
        ncol=4,
        frameon=True,
        fancybox=True,
        framealpha=0.5,
        edgecolor="gray",
        facecolor="white",
        fontsize=6
    )

    fig.autofmt_xdate(rotation=25)
    fig.subplots_adjust(
        top=0.88,
        bottom=0.26
    )

    if file_name is not None:
        folder_name = os.path.dirname(file_name)
        if folder_name:
            os.makedirs(folder_name, exist_ok=True)

        fig.savefig(file_name, dpi=150, bbox_inches="tight")

    if fig_show:
        plt.show()
    else:
        plt.close(fig)
# ----------------------------------------------------------------------------------------------------------------------
