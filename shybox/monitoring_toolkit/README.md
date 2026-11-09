# Monitoring toolkit

The two main topics are **execution** and **resources**. Shared infrastructure
lives in **common**. Ruler is fully integrated under resources, replacing the
previous private collector subset.

```text
monitoring_toolkit/
├── common/                 # JSON/timestamps/configuration
├── execution/              # Lifecycle and run-status analysis
└── resources/              # Process/host RAM, history and plots
    └── ruler/
        ├── handler.py      # Native analysis/workspace/CSV/figure pipeline
        ├── adapter.py      # Ruler fields in resource snapshots
        ├── SOURCE.md       # Revision and integration changes
        └── engine/         # Full namespaced Ruler Python code and MIT licence
```

```python
from shybox.monitoring_toolkit.execution import RunMonitor, MonitoringCatalog
from shybox.monitoring_toolkit.resources import ProcessMemoryMonitor, plot_memory_history
from shybox.monitoring_toolkit.resources.ruler import RulerMonitor, load_ruler_config
```

Execution status records and resource metrics have separate schemas and output
folders. Hydrapp reads both. Ruler's full report driver retains workspace chunks,
CSV output, process selection, sorting/filtering and process/memory/disk figures.
Existing root-level import paths remain small compatibility facades.

See [workflow examples](../../workflow/monitoring/README.md) and the
[monitoring guide](../../docs/monitoring.rst). A complete bounded demo is:

```bash
python -m workflow.monitoring.app_monitoring_ruler_demo --output /tmp/shybox-ruler-demo
```
