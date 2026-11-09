"""Ruler's native analysis/workspace/CSV/plot pipeline within SHYBOX."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path


def load_ruler_config(file_name):
    """Load the original Ruler JSON schema; paths are relative to its file."""
    path = Path(file_name).resolve()
    config = json.loads(path.read_text())
    for key in ('algorithm', 'process', 'report'):
        if not isinstance(config.get(key), dict):
            raise ValueError(f'Ruler configuration requires {key}.')
    for section in ('report', 'log', 'tmp'):
        def resolve(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == 'folder_name' and value:
                        obj[key] = str((path.parent / Path(os.path.expandvars(value)).expanduser()).resolve())
                    else:
                        resolve(value)
        resolve(config.get(section, {}))
    return config


class RulerMonitor:
    """Use the complete Ruler reporting pipeline, independently of lifecycle files.

    Call organize(), dump(), view() separately, or run() for all three. The
    original engine's process-control routines are explicit APIs and are never
    invoked by this reporting wrapper or by Hydrapp.
    """
    def __init__(self, config, *, report_time=None):
        self.config = deepcopy(config)
        flags = self.config['algorithm']['flags']['algorithm']
        if flags.get('activate_report_kill', False):
            raise ValueError('Process control cannot be enabled in a Ruler monitoring report.')
        name = self.config['process'].get('name')
        if not isinstance(name, str) or not name.strip():
            raise ValueError('Ruler process.name must be a nonempty process name or command substring.')
        tools = self.config['process'].get('analysis_tools', [])
        allowed = {'tool_info_memory', 'tool_info_process', 'tool_info_disk', 'tool_info_system_load'}
        if not tools or any(tool not in allowed for tool in tools):
            raise ValueError('Ruler analysis_tools must select supported resource collectors.')
        import matplotlib
        matplotlib.use('Agg')
        from .engine.driver_analysis import DriverAnalysis
        import pandas as pd
        self.driver = DriverAnalysis(
            report_time=pd.Timestamp(report_time or datetime.now(timezone.utc)),
            dict_algorithm=self.config['algorithm'], dict_process=self.config['process'],
            dict_report=self.config['report'], dict_tmp=self.config.get('tmp'))

    def sample(self):
        return self.driver.execute_report_analysis()

    def organize(self):
        self.driver.organize()
        return self

    def dump(self):
        self.driver.dump()
        return self

    def view(self):
        self.driver.view()
        return self

    def run(self):
        self.organize().dump().view()
        path = Path(self.driver.report_file_path_destination)
        return {'csv': str(path), 'available': path.is_file(),
                'workspace_pattern': self.driver.report_file_path_ancillary,
                'figure_pattern': self.driver.report_file_path_figure}


def workspace_to_csv(file_names, destination, *, delimiter=';'):
    """Merge trusted Ruler workspace files into an ordered report CSV."""
    import pandas as pd
    from .engine.lib_data_io import read_obj, write_csv
    frames = []
    for file_name in file_names:
        frame = read_obj(str(file_name))
        if not isinstance(frame, pd.DataFrame):
            raise ValueError(f'Ruler workspace is missing or is not a DataFrame: {file_name}')
        frames.append(frame)
    if not frames:
        raise ValueError('At least one Ruler workspace is required.')
    result = pd.concat(frames).sort_index(kind='stable')
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_csv(str(destination), result, file_separator=delimiter)
    return str(destination)
