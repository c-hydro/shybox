# src/aviso_listen.py
import argparse
import sys
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from pprint import pprint as pp
import shlex

from pyaviso import NotificationManager, user_config
import yaml

# === Globals used by the trigger ===
TARGET_DATE: str | None = None
MAIN_ARGS: list[str] = []   # passthrough args for src/main.py
STOP_EVENT = threading.Event()


def get_repo_root() -> Path:
    """Assume this file is in <repo_root>/src; return <repo_root>."""
    here = Path(__file__).resolve()          # .../repo_root/src/aviso_listen.py
    return here.parent.parent                # .../repo_root


def require_aviso_config(path: str | None) -> dict:
    """
    Load and validate the Aviso listener config.
    """
    if path is None:
        # Default location, but still required to exist
        path = str(get_repo_root() / "configs" / "aviso.yaml")

    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Aviso config file not found: {cfg_path}")

    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # Required top-level keys
    required_top = [
        "notification_engine", "configuration_engine", "schema_parser",
        "remote_schema", "auth_type", "listener_request", "event"
    ]
    missing = [k for k in required_top if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in {cfg_path}: {missing}")

    # listener_request must be a dict; we'll inject the dynamic date later
    if not isinstance(cfg["listener_request"], dict):
        raise ValueError("listener_request must be a mapping in aviso.yaml")

    # Optional: from_date in config (YYYY-MM-DD). CLI can override.
    return cfg


def on_notification(notification: dict) -> None:
    """
    Aviso trigger: handle the incoming notification.
    Uses module-level TARGET_DATE and runs the downloader for that date,
    then signals STOP_EVENT to exit the listener.
    """
    global TARGET_DATE, MAIN_ARGS

    print("=== Aviso notification received ===")
    pp(notification)

    # Extract the date from the notification payload
    rdate = None
    try:
        rdate = notification.get("request", {}).get("date")
    except Exception:
        pass

    if rdate == TARGET_DATE:
        print(f"[aviso] Target date matched: {rdate}. Running downloader...")
        rc = run_downloader_for_date(TARGET_DATE, MAIN_ARGS)
        if rc != 0:
            print(f"[aviso] Downloader returned non-zero exit code: {rc}")
        else:
            print("[aviso] Downloader completed successfully.")
        STOP_EVENT.set()  # exit after handling target date
    else:
        print(f"[aviso] Notification date {rdate} != target {TARGET_DATE}; continue listening.")


def run_downloader_for_date(date_str: str, extra_args: list[str] | None = None) -> int:
    """
    Call your existing downloader as if from CLI:
        python src/main.py --dates <YYYYMMDD> <passthrough main.py args...>

    Using subprocess keeps this script decoupled from downloader internals.
    """
    repo_root = get_repo_root()
    main_py = repo_root / "src" / "main.py"
    cmd = [sys.executable, str(main_py), "--dates", date_str]
    if extra_args:
        cmd.extend(extra_args)

    print("[aviso] Executing:", " ".join(shlex.quote(c) for c in cmd))
    return subprocess.call(cmd)


def parse_args():
    p = argparse.ArgumentParser(
        description="Aviso one-shot listener: run downloader when target date is notified.",
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    p.add_argument("--date", required=False, help="Target date in YYYYMMDD format (default: TODAY, be aware that data might not be available yet).")
    p.add_argument("--aviso-config", required=False, default=None,
                   help="Path to aviso configuration, REQUIRED to exist (default: configs/aviso.yaml).")
    p.add_argument("--from-date", required=False, default=None,
                   help="Override starting point for listening in (YYYY-MM-DD) format (default: read from configs/aviso.yaml).")
    p.add_argument("--timeout-min", type=int, default=None,
                   help="Optional timeout in minutes; exit with code 3 if no matching notification arrives (default: no timeout).")

    # Everything after `--` goes to main.py, untouched.
    p.add_argument(
        "main_args",
        nargs=argparse.REMAINDER,
        help=("Arguments after '--' are passed to src/main.py unchanged. "
              "Example: python src/aviso_listen.py --date 20251107 --timeout-min 90   --   --params-file params/params_example.yaml --keep-cumulative")
    )
    return p.parse_args()


def main():
    global TARGET_DATE, MAIN_ARGS

    args = parse_args()

    # Validate target date
    try:
        TARGET_DATE = datetime.strptime(args.date or datetime.utcnow().strftime("%Y%m%d"), "%Y%m%d").strftime("%Y%m%d")
    except ValueError:
        print("Invalid --date. Expected YYYYMMDD.")
        sys.exit(2)

    # Load and require aviso config
    cfg_dict = require_aviso_config(args.aviso_config)

    # Resolve from_date: CLI overrides config
    if args.from_date:
        try:
            from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
        except ValueError:
            print("Invalid --from-date. Expected YYYY-MM-DD.")
            sys.exit(2)
    else:
        if "from_date" not in cfg_dict:
            print("Missing 'from_date' in aviso config; provide --from-date.")
            sys.exit(2)
        try:
            from_date = datetime.fromisoformat(cfg_dict["from_date"])
        except ValueError:
            print("Invalid 'from_date' in aviso config; expected YYYY-MM-DD.")
            sys.exit(2)

    # Build listener: set dynamic date into the listener_request from config
    listener_request = dict(cfg_dict["listener_request"])
    listener_request["date"] = TARGET_DATE

    triggers = [
        {"type": "echo"},
        {"type": "function", "function": on_notification},  # simple, linear
    ]
    listener = {"event": cfg_dict["event"], "request": listener_request, "triggers": [*triggers]}

    listeners_config = {"listeners": [listener]}
    cfg = user_config.UserConfig(
        notification_engine=cfg_dict["notification_engine"],
        configuration_engine=cfg_dict["configuration_engine"],
        schema_parser=cfg_dict["schema_parser"],
        remote_schema=cfg_dict["remote_schema"],
        auth_type=cfg_dict["auth_type"],
    )

    print("=== Aviso config (sanitized) ===")
    pp({
        "event": cfg_dict["event"],
        "from_date": from_date.isoformat(),
        "listener_request": listener_request,
        "notification_engine": cfg_dict["notification_engine"],
        "configuration_engine": cfg_dict["configuration_engine"],
        "schema_parser": cfg_dict["schema_parser"],
        "remote_schema": cfg_dict["remote_schema"],
        "auth_type": cfg_dict["auth_type"],
    })

    # Prepare passthrough args for main.py.
    # argparse.REMAINDER includes the leading '--' if present; drop it.
    MAIN_ARGS = list(args.main_args or [])
    if MAIN_ARGS and MAIN_ARGS[0] == "--":
        MAIN_ARGS = MAIN_ARGS[1:]

    nm = NotificationManager()

    # Run the listener in a background thread so main thread can control timeout and exit
    t = threading.Thread(
        target=nm.listen,
        kwargs={"listeners": listeners_config, "from_date": from_date, "config": cfg},
        daemon=True,
    )
    t.start()

    # Wait until our on_notification() signals STOP_EVENT, or we hit timeout
    timeout = args.timeout_min * 60 if args.timeout_min else None
    signaled = STOP_EVENT.wait(timeout=timeout)

    if not signaled:
        print("[aviso] Timeout reached; exiting without running downloader.")
        sys.exit(3)

    print("[aviso] Done.")
    sys.exit(0)


if __name__ == "__main__":
    main()
