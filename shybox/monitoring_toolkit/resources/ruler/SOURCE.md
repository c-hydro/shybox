# Ruler integration provenance

Upstream: https://github.com/c-hydro/ruler
Revision: `6fbbf6303620d1d9036a8176d405a3c66ae1d0bd` (2026-11-07 import).
Copyright (c) 2023 CIMA Research Foundation, Department of Hydrology and Hydraulics.
Upstream MIT licence and author/change records are retained in `engine/`.

The complete Python analysis/report/plot/utility implementation is included,
with package-relative imports. SHYBOX integrates the native driver through
`RulerMonitor`, and the resource sampler through `collect_ruler_report`.
Local compatibility fixes cover process-exit races, system memory collection,
headless figure cleanup, current pandas/NumPy APIs, and report separators.
Upstream workstation-specific configurations are retained as reference;
use the runnable SHYBOX workflow examples for portable configurations.

Execution status monitoring is independent of Ruler resource reporting.
Process-control functions remain explicit operations and are not invoked by
monitoring or Hydrapp. The resource report wrapper refuses kill-enabled configs.
