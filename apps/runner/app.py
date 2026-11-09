import json
import os
import sys
import uuid
import threading
from pathlib import Path
from datetime import datetime
from dateutil import parser as date_parser
from flask import Flask, render_template, request, redirect, url_for, Response, flash, send_from_directory

# Paths
APP_DIR = Path(__file__).parent.resolve()
RUNNER_SCRIPT = APP_DIR / "app_runner_workflow_hmc_base_main.py"
BASE_CONFIG = APP_DIR / "app_runner_workflow_hmc_base_example_v316.json"
RUN_DIR = APP_DIR / "runs"
LOG_DIR = RUN_DIR / "logs"
CFG_DIR = RUN_DIR / "cfg"

for p in (RUN_DIR, LOG_DIR, CFG_DIR):
    p.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

# Utilities

def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict, path: Path):
    with path.open("w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def get_nested(d, path_list, default=None):
    cur = d
    for k in path_list:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def set_nested(d, path_list, value):
    cur = d
    for k in path_list[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[path_list[-1]] = value


def infer_input_type(key: str, value):
    if isinstance(value, bool):
        return "checkbox"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        return "text"
    if "time" in key.lower():
        return "datetime"
    if "path" in key.lower() or "file" in key.lower():
        return "text"
    return "text"


def flatten_section(section_dict: dict, base_path):
    rows = []
    for k, v in (section_dict or {}).items():
        full_path = base_path + [k]
        if isinstance(v, dict):
            rows.extend(flatten_section(v, full_path))
        else:
            rows.append({
                "path": ".".join(full_path),
                "key": k,
                "value": v,
                "input": infer_input_type(k, v),
            })
    return rows


def select_editable_fields(cfg: dict):
    sections = []
    env = get_nested(cfg, ["environment"], {})
    sections.append(("Environment", flatten_section(env, ["environment"])))

    by_value = get_nested(cfg, ["application_namelist", "fields", "by_value"], {})
    if by_value:
        sections.append(("Namelist (by_value)", flatten_section(by_value, ["application_namelist", "fields", "by_value"])))

    by_pattern = get_nested(cfg, ["application_namelist", "fields", "by_pattern"], {})
    if by_pattern:
        sections.append(("Namelist (by_pattern)", flatten_section(by_pattern, ["application_namelist", "fields", "by_pattern"])))

    return sections


def parse_form_to_config(base_cfg: dict, form: dict):
    new_cfg = json.loads(json.dumps(base_cfg))
    for field_path, str_val in form.items():
        if field_path in ("_dry_run", "_time_override"):
            continue
        path_list = field_path.split(".")
        old_val = get_nested(new_cfg, path_list)
        if isinstance(old_val, bool):
            val = (str_val.lower() in ("on", "true", "1", "yes"))
        elif isinstance(old_val, int):
            val = int(str_val) if str_val.strip() != "" else old_val
        elif isinstance(old_val, float):
            val = float(str_val.replace(",", ".")) if str_val.strip() != "" else old_val
        else:
            val = str_val
        set_nested(new_cfg, path_list, val)
    return new_cfg


def start_runner_async(job_id: str, cfg_path: Path, time_arg: str, dry_run: bool):
    import subprocess
    log_path = LOG_DIR / f"{job_id}.log"
    cmd = [
        sys.executable,
        str(RUNNER_SCRIPT),
        "-settings_file", str(cfg_path),
        "-time", time_arg
    ]
    env = os.environ.copy()
    with log_path.open("w", buffering=1) as lf:
        lf.write(f"Starting job {job_id}\n")
        lf.write(f"Command: {' '.join(cmd)}\n")
        lf.write(f"Dry-run: {dry_run}\n\n")
        lf.flush()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(APP_DIR), env=env, text=True)
        for line in proc.stdout:
            lf.write(line)
        ret = proc.wait()
        lf.write(f"\nProcess finished with exit code {ret}\n")

# Routes
@app.route("/", methods=["GET"])
def index():
    try:
        cfg = load_config(BASE_CONFIG)
    except Exception as e:
        flash(f"Could not load configuration: {e}", "notification is-danger")
        cfg = {}
    sections = select_editable_fields(cfg)
    return render_template("index.html", sections=sections, raw_cfg=json.dumps(cfg, indent=2))

@app.route("/run", methods=["POST"])
def run_job():
    cfg = load_config(BASE_CONFIG)
    form_dict = dict(request.form)
    new_cfg = parse_form_to_config(cfg, form_dict)

    time_override = request.form.get("_time_override", "").strip()
    if time_override:
        try:
            dt = date_parser.parse(time_override)
            time_arg = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            time_arg = time_override
    else:
        t_env = get_nested(new_cfg, ["environment", "time_run"])
        if t_env:
            try:
                dt = date_parser.parse(str(t_env))
                time_arg = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                time_arg = str(t_env)
        else:
            time_arg = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    job_id = uuid.uuid4().hex[:12]
    cfg_path = CFG_DIR / f"{job_id}.json"
    save_config(new_cfg, cfg_path)
    dry_run = request.form.get("_dry_run") == "on"

    t = threading.Thread(target=start_runner_async, args=(job_id, cfg_path, time_arg, dry_run), daemon=True)
    t.start()

    return redirect(url_for("run_view", job_id=job_id))

@app.route("/run/<job_id>", methods=["GET"])
def run_view(job_id):
    return render_template("run.html", job_id=job_id)

@app.route("/logs/<job_id>", methods=["GET"])
def logs(job_id):
    log_file = LOG_DIR / f"{job_id}.log"
    def generate():
        last_pos = 0
        while True:
            if log_file.exists():
                with log_file.open("r") as f:
                    f.seek(last_pos)
                    chunk = f.read()
                    last_pos = f.tell()
                    if chunk:
                        yield chunk
            break
    return Response(generate(), mimetype="text/plain")

@app.route("/download/<job_id>/config", methods=["GET"])
def download_config(job_id):
    path = CFG_DIR / f"{job_id}.json"
    if not path.exists():
        return "Not found", 404
    return send_from_directory(CFG_DIR, f"{job_id}.json", as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "0")), debug=True)
