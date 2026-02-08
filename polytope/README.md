# Polytope Downloader for FloodPROOFS

Download ECMWF Destination Earth Extremes-DT data nedded for **FloodPROOFS** via *Polytope*. This is done using a small, modular, and config-driven Python toolchain. Integration of the *Aviso* notification system for automatic download at data readiness is also explored.

- **Config lives in** `configs/default.yaml`.
- **Parameter registry** lives in `params/params.yaml`.
- **Outputs** are written to `data/YYYY/MM/DD/param.nc`.

> Authentication for Polytope is required **once** before using the downloader (see below).

Refer also to the official ECMWF repositories for [polytope](https://github.com/ecmwf/polytope) and [polytope-examples](https://github.com/destination-earth-digital-twins/polytope-examples), [aviso](https://github.com/ecmwf/aviso) and [aviso-examples](https://github.com/ecmwf/aviso-examples).

---

## Repository structure

<pre>
. FloodPROOFS-polytope-module
├── configs/
│   ├── default.yaml
│   └── aviso.yaml
├── data/                      # Output directory (example, auto-created)
│   └── 2025/
│       └── 11/
│           └── 10/
│               ├── 2t.nc
│               └── strd.nc
├── params/
│   ├── params.yaml
│   └── params_example.yaml
├── requirements.txt
└── src/
    ├── aviso_listen.py
    ├── config.py
    ├── desp-authentication.py
    ├── downloader.py
    ├── main.py
    ├── params.py
    └── processing.py
</pre>

---


## Initial setup

***Python>=3.10*** is required to run this repo: either install it in your local machine or load it as a module if you are operating in a cluster environment. We make use of a Python virtual environment `venv` to handle the dependencies of Polytope/Aviso. In your local machine, you might need to install it via:  

```
sudo apt install python3-venv.
```

Once `venv` is present in your machine, the environment can be set up via the following commands:

```bash
envname=polytope_env
# Create a virtual environment
python3 -m venv $envname
# Activate it
source $envname/bin/activate
# Upgrade pip
pip install --upgrade pip
# Install dependencies
pip install -r requirements.txt
```

Once the environment is created, you can load it again just via `source polytope_env/bin/activate`.

---

## One-time authentication

Before the first run, authenticate to the Polytope service: this will store your token/credentials locally, by default at `~/.polytopeapirc`. This can be done using the `src/desp-authentication.py` script as:

```
python src/desp-authentication.py -u `<username>` -p `<password>`
```

The `<username>` and `<password>` credentials are those linked to your [Destination Earth platform account](https://platform.destine.eu/). To access these services, you might need to require (and be entitled to) an [upgraded account](https://platform.destine.eu/access-policy-upgrade/). You typically **do not** need to repeat this for subsequent runs.

---


## Configuration

`configs/default.yaml` contains only the **Polytope address** and the  **base request template** for polytope retrieval :

```
address:"polytope.lumi.apps.dte.destination-earth.eu"

base_request:
  class:"d1"
  expver:"0001"
  stream:"oper"
  dataset:"extremes-dt"
  date:"0"
  time:"0000"
  type:"fc"
  grid:"0.05/0.05"
  area:"50.0/2.0/34.0/20.0"
```

> We intentionally **do not** configure output paths here, as `src/config.py` resolves them relative to the repository root:
>
> * `output_dir_base` → `<repo_root>/data`

You can override both at runtime (see below).

---


## Parameters registry

`params/params.yaml` maps parameters shortnames (`2t`, `strd`, etc.) to ECMWF param IDs and metadata, e.g.

```
2t:
  param: "167"
  levtype: "sfc"
  step: "1/2/3"
  type: "instant"

strd:
  param: "175"
  levtype: "sfc"
  step: "0-1/1-2/2-3"
  type: "accum"
```

> Add more entries as needed. For pressure levels, you shall include `levelist` keys.

---


## Usage

From the **repository root**, run:

- All parameters for today (UTC) *(please be aware that data might not be available yet!)*

  ```
  python src/main.py
  ```

- To download data for a specific date run e.g. *(be aware that data for the Extremes-DT is available only up to 15 days in the past!)*

  ```
  python src/main.py --dates 20251110
  ```

By default:

* Outputs go to `data/YYYY/MM/DD/`.
* Parameters are read from `params/params.yaml`.

You can override both:

- Write to a custom base directory

  ```
  python src/main.py --outdir /scratch/polytope_out --date 20251110 --params 2t
  ```

- Use a custom params file

  ```
  python src/main.py --params-file /path/to/my_params.yaml --date 20251110
  ```

> When overriding `--outdir`, the date-based structure is still enforced:
> `/scratch/polydl_out/2025/11/10/2t.nc`.

- Specific params for a specific date (must be contained in the chosen `--parmas-file` or in the default one if not specified)

  ```
  python src/main.py --date 20251110 --params 2t strd
  ```

- Multiple dates

  ```
  python src/main.py --date 20251110 20251111 --params 2t
  ```

- Change area and grid

  ```
  python src/main.py --params 2t --date 20251110 --area "52/0/30/22" --grid "0.1/0.1"
  ```

### Accumulation behavior

By default, cumulative variables (e.g. precipitation or radiation totals) are converted to **interval accumulations** using consecutive step differences. To keep the original cumulative values instead, use:

```
python src/main.py --date 20251110 --params strd --keep-cumulative
```

More info below

---


### Aviso Integration for Event-Driven Downloads

We also integrate **event-driven downloads** for the *Extremes-DT* dataset via Aviso. This allows the downloader to run automatically when a notification for a specific date is received. 

Refer also to the [official aviso examples repository](https://github.com/ecmwf/aviso-examples). As mentioned there, one might need to obtain credentials from the [ECMWF API Page](https://api.ecmwf.int/v1/key/)

#### How it works
- The listener subscribes to Aviso notifications for the `data` event.
- It waits for a notification matching:
  - The **default request filter** (defined in `configs/aviso.yaml`).
  - The **target date** provided on the command line.
- When a matching notification arrives, the listener just **executes the polytope downloader** as above.
- After running the downloader, the listener **exits cleanly**.

#### Usage
Start the listener for a specific date, e.g.

```bash
python src/aviso_listen.py --date 20251110
```

This would listen for notification of the Extremes-DT data readiness for the selected date, and executes the polyotpe downloader as soon as the notification arrives:

```bash
python3 src/main.py --date 20251110
```

If not date is supplied, it defaults to today; *please be aware that data might not be available yet!*


#### Configuration
Aviso connection settings for *Extremes-DT* are in:
```
configs/aviso.yaml
```

You can override this file with `--aviso-config`:

```bash
python src/aviso_listen.py --date 20251110 --aviso-config configs/aviso.yaml
```

All flags available for `src/main.py` can also be used when calling `src/aviso_listen.py`, just list them after a `--` separator. For example:

```
python src/aviso_listen.py --date 20251107 --timeout-min 90   --  --params-file params/params_example.yaml --keep-cumulative
```

It is possible, but not treated here, to listen for other ECMWF notification events. Refer to the official Aviso documentation for further information.

#### Timeout (optional)
By default, the listener waits **indefinitely** for a matching notification. To avoid hanging forever in batch jobs, use `--timeout-min` to exit after a given number of minutes if no notification arrives:
```bash
python src/aviso_listen.py --date 20251110 --timeout-min 120
```
If the timeout expires, the script exits with code `3` without running the downloader.

#### Summary
- **Default behavior:** Wait forever until the notification for the target date arrives.
- **On match:** Run `src/main.py` with `--date <target_date>` and exit.
- **Optional:** Use `--timeout-min` for safety in scheduled jobs.
- **Configurable:** Aviso connection details in `configs/aviso.yaml`.

---

## Accumulated vs Interval (Hourly) Values

Many parameters retrieved from Polytope are **cumulative** fields, where each forecast step contains
the accumulation from **step 0 up to step N** (e.g., total precipitation from model start time to step N).

By **default**, this downloader converts cumulative fields into **per-step intervals** (hourly accumulations)
by differencing consecutive steps (i.e., step N minus step N-1). This is often what downstream hydrology
or energy workflows expect.

- Example: if `strd` or `tp` are marked as `type: accum` in `params.yaml`, the output NetCDF will contain
  **hourly** values by default.

If you prefer to keep the **cumulative values** as they come from the API, simply use:

```
python src/main.py --date 20251110 --params strd --keep-cumulative
```

## Troubleshooting

* **Invalid date format** Make sure `--date` are `YYYYMMDD` (e.g., `20251110`).
* **Authentication errors** If the token is missing/expired, re-run:
  ```
  python src/desp-authentication.py -u `<username>` -p `<password>`
  ```
* **No parameters selected** Ensure your `--params` exist in `params/params.yaml` (check spelling).Or omit `--params` to use **all** available parameters.
* **Paths** The script assumes you run from the  **repo root** . If running elsewhere, either:
* `cd` to the repo root, or use absolute paths for `--config`, `--outdir`, and `--params-file`.

---

## Development tips

* Add new parameters by editing `params/params.yaml` or creating a custom one.
* If you need to experiment with requests, use `--area`, `--grid`, and `--params` flags.

---
