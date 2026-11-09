"""Headless charts for SHYBOX process and host resource history."""
import csv
from pathlib import Path

def plot_memory_history(csv_file, image_file):
    """Headless per-process RSS and host RAM figure, adapted from HAT's plotter."""
    from datetime import datetime
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
    with Path(csv_file).open(encoding='utf-8', newline='') as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError('Memory CSV contains no samples.')
    fig = Figure(figsize=(12, 7), constrained_layout=True)
    FigureCanvasAgg(fig)
    axes = fig.subplots(2, 1, sharex=True)
    for label in dict.fromkeys(row['label'] for row in rows):
        selected = [row for row in rows if row['label'] == label and row['rss_mib']]
        if selected:
            axes[0].plot([datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00')) for row in selected],
                         [float(row['rss_mib']) for row in selected], marker='.', label=label)
    host = {row['timestamp']: row for row in rows}
    axes[1].plot([datetime.fromisoformat(stamp.replace('Z', '+00:00')) for stamp in host],
                 [float(row['host_used_percent']) for row in host.values()], color='#007c88', marker='.')
    axes[0].set(title='Process RAM (RSS)', ylabel='MiB')
    if axes[0].lines:
        axes[0].legend()
    axes[1].set(title='Host RAM (total minus available)', ylabel='%', xlabel='Time (UTC)', ylim=(0, 100))
    locator = AutoDateLocator(); axes[1].xaxis.set_major_locator(locator)
    axes[1].xaxis.set_major_formatter(ConciseDateFormatter(locator))
    for axis in axes:
        axis.grid(alpha=.25)
    Path(image_file).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(image_file, dpi=140)
    return str(image_file)
