# SHYBOX hydrological dashboard

An independent Flask application based on the navigation, CIMA branding, OpenLayers maps and Plotly charts of the unfinished `web/` application. Its assets live inside `dashboard/`. The existing application and scientific package are unchanged.

From the repository root:

```bash
./setup_shybox_flask.sh
dashboard/.venv/bin/python -m dashboard.app --config dashboard/config.json
```

Open **http://127.0.0.1:5050**. Stop with Ctrl+C. The setup script defaults to the existing SHYBOX base Python when present; otherwise `python3`. Python 3.10 or newer is required (3.12 recommended). Override with `--python /path/to/python` and/or `--venv /path/to/environment`. Installing requires internet access. A Linux Python without `venv` support needs its distribution's Python venv package.

For an already activated environment, install `python -m pip install -r dashboard/requirements.txt` and launch `python -m dashboard.app`. `SHYBOX_DASHBOARD_CONFIG` may specify a configuration path; `--config` takes precedence. Bind elsewhere using `--host` and `--port`. The default server is for local use; a deployment should use a WSGI server and `dashboard.app:create_app()`.

## Pages

- **Runs & sections:** declared configurations, date/calendar input, available initialization times, basin filtering, status counts, section map and clickable section table. Choose a calendar date to list runs on that date. Dates between runs may have no results. Refresh rescans the declared folders without restarting.
- **Time series:** basin and section selectors; deterministic analysis/open-loop/observation curves or probabilistic ensemble bands and median. Thresholds and initialization time are marked. All-missing observations are explicitly identified.
- **Gridded maps:** an empty state for soil moisture and discharge. No raster data is fabricated. The gridded reader and publisher still need implementation once a sample is available; setting `maps.enabled` alone does not implement that integration.

Selections are preserved in page links and the URL. OpenLayers 10.6.1 and Plotly 2.34.0 are loaded from CDNs, and the basemap uses OpenStreetMap, so interactive maps/charts require internet access. The section table still works if the map library fails. Dates/times use the values stored in the data; no timezone is assumed or converted.

## Improved JSON configuration

`config.json` replaces the exploratory schema in `web/config.json`. There are no debug dates, hardcoded absolute paths, or legacy aliases. The **runs object is the source of truth**: only enabled, declared configurations appear. Each configuration independently declares mode, expected output location and filename convention. A declaration with no matching files remains visible with “No results available”. Undeclared directories are never published.

The supplied declarations are `marche_deterministic` (connected to the inspected file in `exec/case_study_marche/analyzer_hmc/data/time_series`) and `marche_probabilistic` (an expected ensemble collection, currently empty). Disable or remove the latter if it is not part of your workflow. It does not reinterpret the deterministic file as an ensemble.

| Field | Meaning |
| --- | --- |
| `title` | Browser title |
| `default_run` | Enabled configuration selected initially |
| `missing_values` | Additional numerical missing-data sentinels |
| `overview.period` | `all` timestamps or `forecast` (timestamps at/after initialization) |
| `overview.probability_cutoff` | Ensemble exceedance probability needed to colour a section; in `(0, 1]` |
| `runs.<key>.name`, `enabled` | Display label and publication switch |
| `runs.<key>.mode` | `deterministic` or `probabilistic` |
| `time_series.folder` | Absolute path or path relative to the JSON file |
| `time_series.file_pattern` | Recursive glob beneath that folder |
| `time_series.run_time_regex` | Regex with named `time` group, matched against the relative filename |
| `time_series.run_time_format` | Python `strptime` format for that group |
| `ensemble_dimension` | Required ensemble dimension name for probabilistic simulations |
| `quantiles` | Five increasing probabilities, median `0.5` at index 2; defaults `[.05,.25,.5,.75,.95]` |
| `maps.enabled`, `maps.variables` | Publication intent for future gridded integration; reader not implemented yet |

The filename `hmc.series_marche_202608111200.nc` identifies initialization `2026-08-11 12:00`. Discovery does not open every file: malformed timestamps are reported, and invalid NetCDF schemas produce an explanatory error when selected. Each request opens one file and closes it before returning; no dataset cache is retained. Publish completed files by atomic rename so readers cannot see partially written results.

### NetCDF contract

One NetCDF file represents one initialized run. It must have a sorted, unique datetime `time` coordinate and a `sections` dimension. Metadata variables on `sections`: `id`, `tag`, `section_name`, `basin`, `longitude`, `latitude`, `catchment_area_km2`, and `threshold_level_1/2/3`. Missing metadata has a display fallback; valid coordinates are required for map placement, and thresholds are required for classification.

Supported discharge variables (m³/s):

- `discharge_simulated_analysis`
- `discharge_simulated_openloop`
- `discharge_observed`

At least one must exist. Deterministic discharge uses `(time, sections)` in any order. In probabilistic mode at least one simulated variable must have `(time, sections, <ensemble_dimension>)`; observations remain `(time, sections)`. All simulated products must share the same member coordinate. Separate files per member are not combined by this version: consolidate them into one run dataset first. Ensemble files with a missing dimension are rejected instead of producing misleading probabilities.

Native NetCDF fill values are decoded by xarray. Nonfinite values, configured sentinels, and negative discharge/threshold values are treated as missing; zero is valid. Missing values are serialized as JSON `null`.

### Classification

Deterministic colour uses the maximum across selected discharge products and selected times: green below/equal to all available thresholds, yellow strictly above level 1, orange above level 2, red above level 3. Grey means no usable discharge or no usable thresholds. If some thresholds are missing, classification uses those available; the missing levels remain visibly undefined. “All available series” includes observations for deterministic runs.

For probabilistic results, each member is checked for at least one exceedance over the selected period and selected simulated products. Missing members are excluded from the probability denominator (members with any finite selected data are included). The highest threshold with exceedance probability **greater than or equal to** `probability_cutoff` sets the colour; otherwise green. Popup details report each threshold's probability. Observations are context only and are not counted as ensemble members. The displayed peak is the maximum simulated value across all selected members/times, not a quantile. Chart bands use the configured quantiles at each timestamp, omitting missing members; no minimum-member quality criterion is applied in this first version.

## Verification

```bash
dashboard/.venv/bin/python -m unittest discover -s dashboard/tests -v
```

Tests use generated deterministic and ensemble NetCDFs to check discovery, publication switches, probabilities, missing data, time windows, invalid dimensions, routes and errors. They do not change operational datasets.
