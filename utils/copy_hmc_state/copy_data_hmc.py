#!/usr/bin/env python3

"""
# example
source venv_copy_data_hmc/bin/activate
python copy_data_hmc.py --config copy_data_hmc.json

python copy_data_hmc.py --config copy_data_hmc.json --dry-run

python copy_data_hmc.py --config copy_data_hmc.json --recursive

"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any


def render(template: str, values: Dict[str, str]) -> str:
    return template.format(**values)


def parse_time(time_run: str) -> Dict[str, str]:
    """
    Supported time_run formats:
      2024-10-16
      2024/10/16
      2024-10-16 13
      2024-10-16 13:00
      2024-10-16 13:00:00
      2024-10-16T13:00
      2024-10-16T13:00:00
      2024/10/16 13:00
    """
    fmts = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(time_run, fmt)
            return {
                "yyyy": dt.strftime("%Y"),
                "mm": dt.strftime("%m"),
                "dd": dt.strftime("%d"),
                "HH": dt.strftime("%H"),
                "MM": dt.strftime("%M"),
                "SS": dt.strftime("%S"),
            }
        except ValueError:
            continue
    raise ValueError(
        f"Unsupported time_run format: {time_run}. "
        f"Examples: '2024-10-16', '2024-10-16 13:00', '2024-10-16T13:00'"
    )


def copy_all_files(src_dir: Path, dst_dir: Path, recursive: bool, dry_run: bool) -> int:
    if not src_dir.exists():
        print(f"[WARN] Input path not available, skipping: {src_dir}")
        return 0

    files = (
        (p for p in src_dir.rglob("*") if p.is_file())
        if recursive
        else (p for p in src_dir.iterdir() if p.is_file())
    )

    n = 0
    for src in files:
        rel = src.relative_to(src_dir) if recursive else src.name
        dst = dst_dir / rel

        if dry_run:
            print(f"[DRY-RUN] {src} -> {dst}")
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            print(f"copied {src} -> {dst}")

        n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg: Dict[str, Any] = json.load(f)

    vars_cfg = cfg["vars"]
    domains_cfg = cfg["domains"]

    # resolve time -> "{time}"
    time_parts = parse_time(vars_cfg["time_run"])
    time_str = render(vars_cfg["time_template"], time_parts)
    values = {"time": time_str, **time_parts}

    total = 0

    for domain_name, dcfg in domains_cfg.items():
        print(f"\n=== Domain: {domain_name} | time: {time_str} ===")

        domain_has_data = False

        for mode in ("grid", "point"):
            try:
                src_tpl = dcfg["input"][mode]
                dst_tpl = dcfg["output"][mode]
            except KeyError:
                print(f"[WARN] Missing config keys for {domain_name}.{mode} (need input/output)")
                continue

            src_dir = Path(render(src_tpl, values))
            dst_dir = Path(render(dst_tpl, values))

            n = copy_all_files(src_dir, dst_dir, recursive=args.recursive, dry_run=args.dry_run)
            print(f"{mode}: {n} file(s)")
            total += n
            if n > 0:
                domain_has_data = True

        if not domain_has_data:
            print(f"[WARN] No input data copied for domain '{domain_name}' at time {time_str}")

    print(f"\nDone. Total files processed: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

