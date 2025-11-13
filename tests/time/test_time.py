from shybox.time_toolkit.time_handler import TimeHandler

# 1. Configuration dictionary for your datasets
CONFIG = {
    "timezone": "Europe/Rome",   # processing TZ (your orchestration)
    "data_timezone": "UTC",      # file time axes
    "out": {"prefix": "hmc.forcing.", "suffix": ".nc", "fmt": "%Y%m%d%H%M"},
    "datasets": {
        # Monthly NetCDF with hourly 'time' dimension length = hours_in_month
        "air_temperature": {
            "cadence": "monthly",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/",
            "file_pattern": "temperature_%m%Y.nc"
        },
        "rain": {
            "cadence": "monthly",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/",
            "file_pattern": "rain_%m%Y.nc"
        },
        "incoming_radiation": {
            "cadence": "monthly",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/",
            "file_pattern": "incoming_radiation_%m%Y.nc"
        },
        "relative_humidity": {
            "cadence": "monthly",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/",
            "file_pattern": "relative_humidity_%m%Y.nc"
        },
        "wind_speed": {
            "cadence": "monthly",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/",
            "file_pattern": "wind_speed_%m%Y.nc"
        },
        # Daily LAI raster (reused for all 24 hours)
        "lai": {
            "cadence": "daily",
            "base_dir": "/home/fabio/Desktop/shybox/dset/iwrn/dynamic/LAI/",
            "file_pattern": "%m/%d/CLIM_%m%d_LAI_clip_bbox_ETH.tif"
        }
    }
}

# 3. Instantiate and run
if __name__ == "__main__":

    # Build handler for a cross-month window
    th = TimeHandler(
        start="2000-01-31 18:00",
        end="2000-02-01 06:00",
        config=CONFIG
    )

    # 1) Hours per month (needed by Data classes)
    print(th.month_lengths_hours())
    # -> {'012000': 744, '022000': 696}   # (2000 is leap year, Feb=29 days => 696)

    # 2) Unique monthly NetCDFs to open
    print(th.monthly_manifest())
    # -> {'012000': {'air_temperature': '/.../temperature_012000.nc', ...}, '022000': {...}}

    # 3) Unique LAI files to read
    print(th.lai_manifest())
    # -> ['/.../LAI/01/31/CLIM_0131_....tif', '/.../LAI/02/01/CLIM_0201_....tif']

    # 4) Per-hour resolution (file + hour index inside the monthly cube)
    for rec in th.iterate():
        ts = rec["ts"]
        at = rec["air_temperature"]  # dict with path + hour_index + hours_in_month
        rn = rec["rain"]
        sw = rec["incoming_radiation"]
        rh = rec["relative_humidity"]
        ws = rec["wind_speed"]
        lai_path = rec["lai"]  # absolute daily file path
        out_nc = rec["__output__"]

        # Example: selecting the correct time slice from NetCDF
        # hour_index = at["hour_index"]
        # with xr.open_dataset(at["path"]) as ds:
        #     tstep = ds.isel(time=hour_index)  # time dimension length == at["hours_in_month"]

        # print for sanity
        print(
            f"{ts}: AT[{at['hour_index']}/{at['hours_in_month']}] -> {at['path']}  | LAI -> {lai_path}  | OUT -> {out_nc}")