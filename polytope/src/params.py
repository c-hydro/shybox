from typing import Dict, Any, Iterable, Tuple
import yaml
from pathlib import Path

def load_params(params_file: str) -> Dict[str, Dict[str, Any]]:
    p = Path(params_file)
    if not p.exists():
        raise FileNotFoundError(f"Params file not found: {p}")
    with p.open("r") as f:
        params = yaml.safe_load(f) or {}
    # Expect params keyed by name: {name: {param, levtype, step, type, ...}}
    return params


def select_params(all_params: Dict[str, Dict[str, Any]],
                  requested: Iterable[str] | None) -> Dict[str, Dict[str, Any]]:
    if not requested:
        return all_params
    selected = {}
    for name in requested:
        meta = all_params.get(name)
        if meta:
            selected[name] = meta
    return selected

def iter_param_requests(base_request: Dict[str, Any],
                        date_str: str,
                        params: Dict[str, Dict[str, Any]]):
    """
    Yield (name, param, request, meta) for each parameter.
    """
    for name, meta in params.items():
        param = meta["param"]
        req = dict(base_request)
        req.update({
            "param": param,
            "date": date_str,
            "levtype": meta["levtype"],
            "step": meta["step"],
        })
        # Optional level keys
        for k in ("level", "levelist"):
            if k in meta:
                req[k] = meta[k]
        yield name, param, req, meta