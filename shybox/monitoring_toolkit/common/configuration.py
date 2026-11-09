"""Resolve shared monitoring reader configuration."""
from copy import deepcopy
from pathlib import Path

def resolve_monitoring_config(config, base_folder):
    """Resolve reader paths relative to the JSON configuration location."""
    config = deepcopy(config or {})
    def resolve(value):
        return str((Path(base_folder) / Path(value).expanduser()).resolve())
    if config.get('folder'):
        config['folder'] = resolve(config['folder'])
    for key in ('memory_file', 'memory_plot'):
        if config.get(key):
            config[key] = resolve(config[key])
    for spec in config.get('applications', {}).values():
        if spec.get('legacy'):
            legacy = spec['legacy']
            legacy['start_file'] = resolve(legacy['start_file'])
            legacy['output_files'] = [resolve(value) for value in legacy.get('output_files', [])]
    return config


