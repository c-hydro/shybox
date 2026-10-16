"""
Library Features:

Name:          lib_proc_interpolate_hs
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261016'
Version:       '1.1.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import pandas as pd
import xarray as xr
import rasterio

from scipy.optimize import least_squares
from scipy.interpolate import griddata

from shybox.processing_points_toolkit.lib_utils_interpolate_hs import (
    prepare_raster, evaluate_quality, interpolazione, kernel, regression_function,
)

# common decorators
from shybox import with_logger
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to interpolate hs points to grid
def interp_hsnow(
        HS_df: pd.DataFrame,
        SCA_da: xr.DataArray,
        CLOUD_da: xr.DataArray,
        DEM_da: xr.DataArray,
        Bianco_da: xr.DataArray,
        Paradiso_da: xr.DataArray,
        Combin_da: xr.DataArray,
        foreste_da: xr.DataArray,
        S3M_threshold_cloud_per: float = 3,
        S3M_Rregression: float = 300,
        S3M_minimum_dN_for_Updating: int = 20,
        S3M_R2_min: float = 0.2,
        S3M_R2_high_elev: float = 0.6,
        S3M_Thresh_high_elev: float = 2500,
        S3M_Updating_coeff_corr_Combin: float = 1,
        S3M_Corr_z_Updating: tuple = (2000, 4500),
        fill_value: float = -9999.0,
        no_data_value: float = -9999.0,
        **kwargs,):

    logger_stream = kwargs.get("logger_stream", None)
    if logger_stream is None:
        logger_stream = logging.getLogger(__name__)
    logger_stream.info("Starting snow height interpolation")

    DEM = prepare_raster(DEM_da)
    Bianco = prepare_raster(Bianco_da)
    Paradiso = prepare_raster(Paradiso_da)
    Mappa_corr_Combin = prepare_raster(Combin_da)
    mask_foreste = prepare_raster(foreste_da)

    for arr in (DEM, Bianco, Paradiso, Mappa_corr_Combin, mask_foreste):
        if not np.isnan(no_data_value):
            arr[arr == no_data_value] = np.nan

    Ref_DEM = rasterio.Affine(*DEM_da.attrs["transform"][:6])
    Ref_foreste = rasterio.Affine(*foreste_da.attrs["transform"][:6])
    DEM_crs = DEM_da.attrs["crs"]
    DEM_profile = DEM_da.attrs["profile"]

    if SCA_da is not None and CLOUD_da is not None:
        a2dSCA = SCA_da.to_numpy().astype(float, copy=True)
        a2dCloud = CLOUD_da.to_numpy().astype(float, copy=True)
        if a2dSCA.shape != DEM.shape or a2dCloud.shape != DEM.shape:
            logger_stream.error("SCA/CLOUD grid shape mismatch with DEM: SCA=%s CLOUD=%s DEM=%s", a2dSCA.shape,
                                a2dCloud.shape, DEM.shape)
            raise ValueError("DEM, SCA e CLOUD devono avere la stessa forma")
        a2dSCA[(a2dCloud > S3M_threshold_cloud_per) & (DEM > 0)] = -1
        sFlagSCA = "ok"
        logger_stream.info("SCA and CLOUD available: cloud threshold=%s, masked pixels=%d; satellite SCA retained",
                           S3M_threshold_cloud_per,
                           int(np.count_nonzero((a2dCloud > S3M_threshold_cloud_per) & (DEM > 0))))
    else:
        a2dSCA = np.full(DEM.shape, -1.0, dtype=float)
        a2dSCA[np.isnan(DEM)] = np.nan
        sFlagSCA = "no"
        logger_stream.warning("SCA and/or CLOUD unavailable: SCA initialized to -1 on valid DEM pixels")

    required_columns = ("HSavg", "elev", "lon", "lat")
    if HS_df is None or not isinstance(HS_df, pd.DataFrame):
        logger_stream.error("HS_df must be a pandas DataFrame, got %s", type(HS_df).__name__)
        raise TypeError("HS_df must be a pandas DataFrame")
    missing_columns = [col for col in required_columns if col not in HS_df.columns]
    if missing_columns:
        logger_stream.error("Missing snow observation columns: %s", missing_columns)
        raise ValueError(
            f"Colonne mancanti nel DataFrame nivometrico: {missing_columns}. "
            f"Colonne disponibili: {list(HS_df.columns)}"
        )

    if HS_df.empty:
        logger_stream.warning("Snow observations DataFrame empty: regression will be skipped")
        dN = np.array([np.nan])
        dLonN = np.array([np.nan])
        dLatN = np.array([np.nan])
        dElevN = np.array([np.nan])
    else:
        dN = pd.to_numeric(HS_df["HSavg"], errors="coerce").to_numpy(dtype=float)
        dLonN = pd.to_numeric(HS_df["lon"], errors="coerce").to_numpy(dtype=float)
        dLatN = pd.to_numeric(HS_df["lat"], errors="coerce").to_numpy(dtype=float)
        dElevN = pd.to_numeric(HS_df["elev"], errors="coerce").to_numpy(dtype=float)
        # dElevN e' disponibile, ma la regressione usa ancora la quota del DEM.
    logger_stream.info("Input loading completed")

    # Definizione funzione di regressione per HS
    b0 = np.array([1.0, 1.0, 1.0, 0.0])

    a2dQuality = evaluate_quality(a2dSCA)
    logger_stream.info("SCA quality map computed (SCA status=%s)", sFlagSCA)

    # Lettura dati nivometrici

    # Se i dati sono maggiori della soglia minima, procediamo
    n_observations = len(HS_df)
    if n_observations > S3M_minimum_dN_for_Updating:
        logger_stream.info("Snow station count check passed: %d > %d; preparing regression predictors", n_observations,
                           S3M_minimum_dN_for_Updating)

        # Carichiamo predittori

        # Coordinate dei centri delle celle del DEM
        rows, cols = np.indices(DEM.shape)
        Lon, Lat = rasterio.transform.xy(
            Ref_DEM,
            rows,
            cols,
            offset="center",
        )
        Lon = np.asarray(Lon).reshape(DEM.shape)
        Lat = np.asarray(Lat).reshape(DEM.shape)

        # Bianco
        # Prendiamo i valori dei predittori alle località dei dati
        X = np.full(
            (len(dN), 3),
            np.nan,
            dtype=float,
        )
        for i in range(len(dN)):
            if np.isnan(dLatN[i]) or np.isnan(dLonN[i]):
                continue
            row, col = rasterio.transform.rowcol(
                Ref_DEM,
                dLonN[i],
                dLatN[i],
            )
            if 0 <= row < DEM.shape[0] and 0 <= col < DEM.shape[1]:
                X[i, 0] = DEM[row, col]
                X[i, 1] = Bianco[row, col]
                X[i, 2] = Paradiso[row, col]

        # Remove invalid observations and validate regression inputs.
        valid = np.isfinite(dN) & np.isfinite(X).all(axis=1)
        dN_noNaN = dN[valid]
        X_noNaN = X[valid, :]
        n_valid = len(dN_noNaN)
        logger_stream.info("Valid regression observations: %d/%d (finite HSavg and DEM/Bianco/Paradiso)", n_valid,
                           n_observations)
        if n_valid <= S3M_minimum_dN_for_Updating:
            logger_stream.warning(
                "Valid observation check failed: %d <= %d; Kernel and SnowHeight set to fill_value=%s", n_valid,
                S3M_minimum_dN_for_Updating, fill_value)
            regression_accepted = False
        else:
            logger_stream.info("Valid observation check passed: %d > %d; checking observed variance", n_valid,
                               S3M_minimum_dN_for_Updating)
            SStot = np.sum((dN_noNaN - np.mean(dN_noNaN)) ** 2)
            if not np.isfinite(SStot) or SStot <= 0:
                logger_stream.warning(
                    "SStot check failed: SStot=%s; R2 undefined, regression skipped; Kernel and SnowHeight set to fill_value=%s",
                    SStot, fill_value)
                regression_accepted = False
            else:
                logger_stream.info("SStot check passed: SStot=%.6g > 0; running least-squares regression", SStot)

                def residual_function(b):
                    return regression_function(b, X_noNaN) - dN_noNaN

                fit_result = least_squares(residual_function, b0)
                b_all = fit_result.x
                simulated = regression_function(b_all, X_noNaN)
                SSres = np.sum((dN_noNaN - simulated) ** 2)
                Rsq = 1 - SSres / SStot
                logger_stream.info(
                    "Regression result: success=%s, R2=%.6g (threshold=%s), b_all[0]=%.6g (required > 0)",
                    fit_result.success, Rsq, S3M_R2_min, b_all[0])
                r2_ok = bool(np.isfinite(Rsq) and Rsq > S3M_R2_min)
                coefficient_ok = bool(np.isfinite(b_all[0]) and b_all[0] > 0)
                if r2_ok:
                    logger_stream.info("R2 check passed: %.6g > %s", Rsq, S3M_R2_min)
                else:
                    logger_stream.warning("R2 check failed: R2=%s, required > %s", Rsq, S3M_R2_min)
                if coefficient_ok:
                    logger_stream.info("Regression coefficient check passed: b_all[0]=%.6g > 0", b_all[0])
                else:
                    logger_stream.warning("Regression coefficient check failed: b_all[0]=%s, required > 0", b_all[0])
                regression_accepted = r2_ok and coefficient_ok
                if regression_accepted:
                    logger_stream.info("Regression accepted: computing SnowHeight and Kernel")
                else:
                    logger_stream.warning("Regression rejected: Kernel and SnowHeight set to fill_value=%s", fill_value)
        if regression_accepted:
            # Prima versione della mappa
            predictors_grid = np.column_stack(
                (
                    DEM.ravel(),
                    Bianco.ravel(),
                    Paradiso.ravel(),
                )
            )
            dN_map = regression_function(
                b_all,
                predictors_grid,
            )
            dN_map = dN_map.reshape(
                DEM.shape
            )
            dN_map[dN_map < 0] = 0

            # Correzione Combin
            dN_map = (
                    dN_map
                    + dN_map
                    * S3M_Updating_coeff_corr_Combin
                    * Mappa_corr_Combin
            )

            # Calcolo residui
            dN_map_ai_nivo = np.full(
                len(dN),
                np.nan,
                dtype=float,
            )
            for i in range(len(dN)):
                if np.isnan(dLatN[i]) or np.isnan(dLonN[i]):
                    continue
                row, col = rasterio.transform.rowcol(
                    Ref_DEM,
                    dLonN[i],
                    dLatN[i],
                )
                if 0 <= row < DEM.shape[0] and 0 <= col < DEM.shape[1]:
                    dN_map_ai_nivo[i] = (
                        dN_map[row, col]
                    )
            residui = (
                    dN
                    - dN_map_ai_nivo
            )
            Residui = interpolazione(
                DEM,
                Lon,
                Lat,
                dLonN,
                dLatN,
                X[:, 0],
                residui,
                2,
            )
            dN_map = (
                    dN_map
                    + Residui
            )
            dN_map[dN_map < 0] = 0

            # Unità di misura: passo a mm
            dN_map = dN_map * 10

            # Calcolo Kernel
            dYU = np.nanmax(Lat)
            dXL = np.nanmin(Lon)
            pixel_size = abs(
                Ref_DEM.a
            )
            posy = np.ceil(
                (dYU - dLatN)
                / pixel_size
            ).astype(int)
            posx = np.ceil(
                (dLonN - dXL)
                / pixel_size
            ).astype(int)
            Kernel = kernel(
                posx,
                posy,
                DEM,
                S3M_Rregression,
            )

            # Correzione con mappa canopy
            rows_foreste, cols_foreste = np.indices(
                mask_foreste.shape
            )
            Lon_foreste, Lat_foreste = rasterio.transform.xy(
                Ref_foreste,
                rows_foreste,
                cols_foreste,
                offset="center",
            )
            Lon_foreste = np.asarray(Lon_foreste)
            Lat_foreste = np.asarray(Lat_foreste)
            mask_foreste_resampled = griddata(
                (
                    Lon_foreste.ravel(),
                    Lat_foreste.ravel(),
                ),
                mask_foreste.ravel(),
                (
                    Lon,
                    Lat,
                ),
            )
            mask_foreste_resampled[
                np.isnan(mask_foreste_resampled)
                & (DEM > 0)
                ] = 0
            Kernel = (
                    Kernel
                    * (
                            1
                            - mask_foreste_resampled / 100
                    )
            )

            # Correzione Kernel con quota
            z_min = S3M_Corr_z_Updating[0]
            z_max = S3M_Corr_z_Updating[1]
            mask = (
                    (DEM >= z_min)
                    & (DEM <= z_max)
            )
            Kernel[mask] = (
                    Kernel[mask]
                    * (
                            (z_max - DEM[mask])
                            / (z_max - z_min)
                    )
            )

            # Correzione alte quote in funzione di R2
            if Rsq < S3M_R2_high_elev:
                high_mask = DEM >= S3M_Thresh_high_elev
                Kernel[high_mask] = 0
                logger_stream.info(
                    "High-elevation check triggered: R2=%.6g < %s; Kernel set to zero for %d pixels at elevation >= %s",
                    Rsq, S3M_R2_high_elev, int(np.count_nonzero(high_mask)), S3M_Thresh_high_elev)
            else:
                logger_stream.info(
                    "High-elevation check not triggered: R2=%.6g >= %s; Kernel unchanged by this correction", Rsq,
                    S3M_R2_high_elev)

            # NODATA
            dN_map[np.isnan(DEM)] = fill_value
            Kernel[np.isnan(DEM)] = fill_value
            dN_map[np.isnan(dN_map)] = fill_value
        else:
            Kernel = np.full_like(DEM, fill_value)
            dN_map = np.full_like(DEM, fill_value)
    else:
        logger_stream.warning(
            "Snow station count check failed: %d <= %d; regression skipped, Kernel and SnowHeight set to fill_value=%s",
            n_observations, S3M_minimum_dN_for_Updating, fill_value)
        Kernel = np.full_like(DEM, fill_value)
        dN_map = np.full_like(DEM, fill_value)

    # Conversione dei risultati in DataArray sulla griglia del DEM.
    def as_dataarray(values, name):
        da = DEM_da.copy(data=np.asarray(values).copy())
        da.name = name
        da.attrs = DEM_da.attrs.copy()
        da.attrs["nodata"] = fill_value
        da.attrs["_FillValue"] = fill_value
        return da

    logger_stream.info("Snow height interpolation completed: returning SCA, SQA, Kernel and SnowHeight")
    return (
        as_dataarray(a2dSCA, "SCA"),
        as_dataarray(a2dQuality, "SQA"),
        as_dataarray(Kernel, "Kernel"),
        as_dataarray(dN_map, "SnowHeight"),
    )
# ----------------------------------------------------------------------------------------------------------------------
