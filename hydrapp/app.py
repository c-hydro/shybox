"""Launch with: python -m hydrapp.app --config hydrapp/config.json."""
import argparse
import logging
import os
from pathlib import Path

from flask import Response, Flask, jsonify, render_template, request, send_file, redirect, url_for

from hydrapp.data import Catalog, DataError, SERIES, load_config
from shybox.monitoring_toolkit.execution import MonitoringCatalog
from shybox.monitoring_toolkit.resources import read_memory_snapshot
from shybox.publishing_toolkit import LegacyFigureCatalog


def create_app(config_path=None):
    app = Flask(__name__)
    cfg = load_config(config_path or os.environ.get("HYDRAPP_CONFIG") or os.environ.get("SHYBOX_DASHBOARD_CONFIG")
                      or Path(__file__).with_name("config.json"))
    catalog = Catalog(cfg)
    monitoring = MonitoringCatalog(cfg.get("monitoring"))
    publication_config = cfg.get('publications', {})
    figures_catalog = LegacyFigureCatalog(publication_config.get('folder'), enabled=publication_config.get('enabled', False))

    @app.context_processor
    def shared_context():
        return {'publications_enabled': figures_catalog.enabled, 'demo_mode': cfg.get('demo', False)}
    app.config["DASHBOARD"] = cfg
    # Reject accidental non-standard NaN/Infinity JSON values.
    original_dumps = app.json.dumps
    app.json.dumps = lambda obj, **kwargs: original_dumps(obj, **{**kwargs, "allow_nan": False})

    @app.get("/")
    @app.get("/runs")
    @app.get("/time-series")
    @app.get("/maps")
    def page():
        if request.path == '/' and figures_catalog.enabled:
            return redirect('/figures')
        if request.path == '/maps' and figures_catalog.enabled:
            return render_template('figures.html', kind='maps', title=cfg.get('title', 'Hydrapp'))
        page_name = {"/": "overview", "/runs": "overview", "/time-series": "series", "/maps": "maps"}[request.path]
        return render_template("dashboard.html", page=page_name,
                               title=cfg.get("title", "Hydrapp"),
                               defaults={"collection": cfg["default_run"],
                                         "period": cfg["overview"]["period"]})

    @app.get('/figures')
    @app.get('/figures/time-series')
    def figures():
        kind = 'time_series' if request.path.endswith('/time-series') else 'all'
        return render_template('figures.html', kind=kind, title=cfg.get('title', 'Hydrapp'))

    @app.get('/api/figures')
    def published_figures():
        data = figures_catalog.discover()
        for product in data['products']:
            product['image_url'] = url_for('figure_asset', product_id=product['id'], asset='image')
            product['metadata_url'] = url_for('figure_asset', product_id=product['id'], asset='metadata')
        return jsonify(data)

    @app.get('/api/figures/<product_id>/<asset>')
    def figure_asset(product_id, asset):
        return read_result(lambda: send_file(figures_catalog.resolve(product_id, asset),
                                             as_attachment=request.args.get('download') == '1', conditional=True))

    @app.get("/monitoring")
    def monitoring_page():
        return render_template("monitoring.html", title=cfg.get("title", "Hydrapp"))

    @app.get('/api/monitoring/memory')
    def memory_summary():
        options = cfg.get('monitoring', {})
        return read_result(lambda: read_memory_snapshot(options.get('memory_file'), options.get('stale_seconds', 180)))

    @app.get('/api/monitoring/memory/plot')
    def memory_plot():
        file_name = cfg.get('monitoring', {}).get('memory_plot')
        if not file_name or not Path(file_name).is_file():
            return jsonify(error='Memory figure unavailable.'), 404
        return send_file(file_name, conditional=True)

    @app.get("/api/monitoring")
    def monitoring_summary():
        return read_result(lambda: monitoring.summary(date=request.args.get('date'),
                                                      application=request.args.get('application') or None))

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
            result = callback()
            return result if isinstance(result, Response) else jsonify(result)
        except KeyError as exc:
            return jsonify(error=exc.args[0]), 404
        except DataError as exc:
            return jsonify(error=str(exc)), 422
        except (OSError, ValueError, TypeError, IndexError) as exc:
            app.logger.exception("Unable to read a dashboard result")
            return jsonify(error="Unable to read this result. Check the configured source and server log."), 422

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hydrapp hydrological dashboard")
    parser.add_argument("--config", help="JSON configuration path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    create_app(args.config).run(host=args.host, port=args.port, debug=False)
