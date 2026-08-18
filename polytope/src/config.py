# src/config.py
import os
import yaml

def get_repo_root():
    """
    Return the repo root, assumed to be the parent of the directory that contains this file.
    If this file lives in <repo_root>/src/config.py, repo_root = dirname(<repo_root>/src).
    """
    here = os.path.abspath(os.path.dirname(__file__))   # .../repo_root/src
    return os.path.abspath(os.path.join(here, ".."))    # .../repo_root

def load_config(config_file: str | None = None) -> dict:
    """
    Load YAML config (for address and base_request) and resolve output/params paths
    relative to the repository root, as requested.

    - config_file defaults to 'configs/default.yaml' under the repo root.
    - cfg['output_dir_base'] := <repo_root>/data
    - cfg['params_file']     := <repo_root>/params/params.yaml
    """
    repo_root = get_repo_root()
    if config_file is None:
        config_file = "configs/default.yaml"

    # If the provided config_file is relative, resolve it from repo_root
    config_path = config_file
    if not os.path.isabs(config_path):
        config_path = os.path.join(repo_root, config_file)

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # Always resolve these two relative to repo root (per your requirement)
    cfg["output_dir_base"] = os.path.join(repo_root, "data")
    cfg["params_file"] = os.path.join(repo_root, "params", "params.yaml")

    # Basic validation for required keys that come from YAML
    required = ["address", "base_request"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in {config_path}: {missing}")

    return cfg