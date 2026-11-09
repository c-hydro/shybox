# Hydrapp demonstration

`figures/` contains 23 original HAT PNG/JSON pairs copied from Fabio's legacy
Hydrapp example archive (`/home/fabio/Desktop/shybox/example/figures`). The subset
contains 10 maps (ICON/ECMWF 3/6/12/24-hour rainfall and HMC soil moisture) and
13 section charts (Foglia: Mercatale, Bronzo, Montecchio and Foglia3). Publication
hours are retained. Images and metadata have not been edited. These are archived
products from 21 October 2026, not current model results.

Generate synthetic run statuses plus real RAM/Ruler measurements of the demo:

```bash
python -m workflow.monitoring.app_monitoring_status_demo --duration 60 --resources
```

In another terminal, from the repository root:

```bash
hydrapp/.venv/bin/python -m hydrapp.app --config hydrapp/config.demo.json
```

Open http://127.0.0.1:5050 for figures and `/monitoring` for statuses and RAM.
The analyzer heartbeat stays live while the generator runs. The other statuses
are deliberate examples, including a stale heartbeat, failed member and missing
expected run. Run with `--duration 0 --resources` to generate a quick snapshot.
Generated status UUIDs and resource history may be removed to reset demo history;
never clear an operational monitoring directory for this purpose.
