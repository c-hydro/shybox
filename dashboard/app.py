"""Launch with: python -m dashboard.app --config dashboard/config.json."""
import argparse
import logging
import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from dashboard.data import Catalog, DataError, SERIES, load_config


def create_app(config_path=None):
    app = Flask(__name__)
    cfg = load_config(config_path or os.environ.get("SHYBOX_DASHBOARD_CONFIG")
                      or Path(__file__).with_name("config.json"))
    catalog = Catalog(cfg)
    app.config["DASHBOARD"] = cfg
    # Reject accidental non-standard NaN/Infinity JSON values.
    original_dumps = app.json.dumps
    app.json.dumps = lambda obj, **kwargs: original_dumps(obj, **{**kwargs, "allow_nan": False})

    @app.get("/")
    @app.get("/time-series")
    @app.get("/maps")
    def page():
        page_name = {"/": "overview", "/time-series": "series", "/maps": "maps"}[request.path]
        return render_template("dashboard.html", page=page_name,
                               title=cfg.get("title", "SHYBOX Dashboard"),
                               defaults={"collection": cfg["default_run"],
                                         "period": cfg["overview"]["period"]})

    @app.get("/api/runs")
    def runs():
        return jsonify(collections=catalog.discover(), default=cfg["default_run"])

    @app.get("/api/runs/<collection_id>/<run_id>")
    def summary(collection_id, run_id):
        period = request.args.get("period", cfg["overview"]["period"])
        source = request.args.get("source", "all")
        if period not in ("all", "forecast") or source not in ("all", *SERIES):
            return jsonify(error="Invalid period or discharge series."), 400
        return read_result(lambda: catalog.summary(collection_id, run_id, period, source))

    @app.get("/api/runs/<collection_id>/<run_id>/sections/<int:index>")
    def section(collection_id, run_id, index):
        return read_result(lambda: catalog.series(collection_id, run_id, index))

    def read_result(callback):
        try:
            return jsonify(callback())
        except KeyError as exc:
            return jsonify(error=exc.args[0]), 404
        except DataError as exc:
            return jsonify(error=str(exc)), 422
        except (OSError, ValueError, TypeError, IndexError) as exc:
            app.logger.exception("Unable to read a dashboard result")
            return jsonify(error="Unable to read this result. Check the NetCDF schema and server log."), 422

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SHYBOX hydrological dashboard")
    parser.add_argument("--config", help="JSON configuration path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    create_app(args.config).run(host=args.host, port=args.port, debug=False)
