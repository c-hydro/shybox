#!/usr/bin/env python3
"""Utility per interpolazione neve."""
import numpy as np
import rasterio
from scipy.interpolate import griddata


def prepare_raster(da):
    arr = da.to_numpy().astype(float, copy=True)
    nodata = da.attrs.get("nodata")
    if nodata is not None:
        if np.isnan(nodata):
            arr[np.isnan(arr)] = np.nan
        else:
            arr[arr == nodata] = np.nan
    return arr


def evaluate_quality(a2dData):
    # Funzione che calcola la qualità della SCA

    quality_value = (
            np.sum((a2dData == 1) | (a2dData == 0))
            / np.sum(a2dData > -10)
    )

    a2dQuality = (
            np.ones(a2dData.shape)
            * quality_value
    )

    return a2dQuality


def interpolazione(
        z,
        lon_g,
        lat_g,
        afLongitudini,
        afLatitudini,
        afAltitudini,
        VettOsservazioni,
        TipoInterp,
):
    # Calcola la distanza di ogni centralina
    #
    # TipoInterp == 1: regressione sulle quote + IDW
    # TipoInterp == 2: IDW

    m, n = z.shape

    SumInvDist2 = np.zeros(
        (m, n),
        dtype=float,
    )

    mappa = np.zeros(
        (m, n),
        dtype=float,
    )

    VettOsservazioni = np.asarray(
        VettOsservazioni,
        dtype=float,
    ).copy()

    afAltitudini = np.asarray(
        afAltitudini,
        dtype=float,
    ).copy()

    afLongitudini = np.asarray(
        afLongitudini,
        dtype=float,
    )

    afLatitudini = np.asarray(
        afLatitudini,
        dtype=float,
    )

    VettOsservazioni[
        VettOsservazioni <= -9000
        ] = np.nan

    afAltitudini[
        afAltitudini <= -9000
        ] = np.nan

    # =========================================================================
    # Selezione centraline valide
    # =========================================================================

    if TipoInterp == 1:

        # Nel caso della regressione sulle quote
        # deve essere disponibile anche la quota del sensore.

        kk = (
                np.isfinite(
                    VettOsservazioni
                    + afAltitudini
                )
                & (afLongitudini > 0)
                & (afLatitudini > 0)
        )

    else:

        kk = (
                np.isfinite(VettOsservazioni)
                & (afLongitudini > 0)
                & (afLatitudini > 0)
        )

    VettOsservazioniPulite = (
        VettOsservazioni[kk]
    )

    VettQuotePulite = (
        afAltitudini[kk]
    )

    VettLatPulite = (
        afLatitudini[kk]
    )

    VettLonPulite = (
        afLongitudini[kk]
    )

    iTotCentraline = len(
        VettOsservazioniPulite
    )

    # =========================================================================
    # Nessuna centralina valida
    # =========================================================================

    if iTotCentraline == 0:
        mappa = np.full(
            z.shape,
            np.nan,
            dtype=float,
        )

        return mappa

    # =========================================================================
    # Funzione interna IDW
    # =========================================================================

    def interpIDW(
            VettOsservazioniPulite,
            VettLonPulite,
            VettLatPulite,
    ):

        SumInvDist2 = np.zeros(
            (m, n),
            dtype=float,
        )

        mappa = np.zeros(
            (m, n),
            dtype=float,
        )

        # ---------------------------------------------------------------------
        # Primo ciclo:
        # calcolo della somma degli inversi delle distanze al quadrato
        # ---------------------------------------------------------------------

        for k in range(iTotCentraline):
            same_position = (
                    (VettLonPulite[k] - lon_g == 0)
                    & (VettLatPulite[k] - lat_g == 0)
            )

            dist = (
                    (VettLonPulite[k] - lon_g) ** 2
                    + (VettLatPulite[k] - lat_g) ** 2
            )

            dist[same_position] = 1

            SumInvDist2 = (
                    SumInvDist2
                    + 1.0 / dist
            )

        # ---------------------------------------------------------------------
        # Secondo ciclo:
        # applicazione dei pesi IDW
        # ---------------------------------------------------------------------

        for k in range(iTotCentraline):
            same_position = (
                    (VettLonPulite[k] - lon_g == 0)
                    & (VettLatPulite[k] - lat_g == 0)
            )

            dist = (
                    (VettLonPulite[k] - lon_g) ** 2
                    + (VettLatPulite[k] - lat_g) ** 2
            )

            dist[same_position] = 1

            mappa = (
                    mappa
                    + VettOsservazioniPulite[k]
                    * (1.0 / dist)
                    / SumInvDist2
            )

        return mappa

    # =========================================================================
    # Tipo di interpolazione
    # =========================================================================

    if TipoInterp == 0:

        mappa = griddata(
            points=(
                VettLonPulite,
                VettLatPulite,
            ),
            values=VettOsservazioniPulite,
            xi=(
                lon_g,
                lat_g,
            ),
            method="nearest",
        )

        with np.errstate(
                divide="ignore",
                invalid="ignore",
        ):

            mappa = (
                    mappa
                    * z
                    / z
            )


    elif TipoInterp == -1:

        mappa = griddata(
            points=(
                VettLonPulite,
                VettLatPulite,
            ),
            values=VettOsservazioniPulite,
            xi=(
                lon_g,
                lat_g,
            ),
            method="linear",
        )

        with np.errstate(
                divide="ignore",
                invalid="ignore",
        ):

            mappa = (
                    mappa
                    * z
                    / z
            )


    elif TipoInterp == -3:

        mappa = griddata(
            points=(
                VettLonPulite,
                VettLatPulite,
            ),
            values=VettOsservazioniPulite,
            xi=(
                lon_g,
                lat_g,
            ),
            method="cubic",
        )

        with np.errstate(
                divide="ignore",
                invalid="ignore",
        ):

            mappa = (
                    mappa
                    * z
                    / z
            )


    elif TipoInterp == -4:

        raise NotImplementedError(
            "TipoInterp = -4 usa il metodo MATLAB griddata 'v4', "
            "che non ha un equivalente diretto in scipy.interpolate.griddata."
        )


    elif TipoInterp == 1:

        # =====================================================================
        # Regressione lineare sulle quote
        # =====================================================================

        pq = np.polyfit(
            VettQuotePulite,
            VettOsservazioniPulite,
            1,
        )

        f = np.polyval(
            pq,
            VettQuotePulite,
        )

        VettOsservazioniPulite = (
                VettOsservazioniPulite
                - f
        )

        # IDW sui residui

        mappa = interpIDW(
            VettOsservazioniPulite,
            VettLonPulite,
            VettLatPulite,
        )

        ff = np.polyval(
            pq,
            z,
        )

        mappa = (
                mappa
                + ff
        )


    elif TipoInterp == 2:

        # =====================================================================
        # IDW
        # =====================================================================

        mappa = interpIDW(
            VettOsservazioniPulite,
            VettLonPulite,
            VettLatPulite,
        )


    else:

        raise ValueError(
            f"TipoInterp non riconosciuto: {TipoInterp}"
        )

    return mappa


# Updating_Kernel.py
def kernel(posx, posy, dem, R):
    # Calcolo del peso di ogni nivometro su ogni nodo della griglia
    # d'interpolazione.

    # =========================================================================
    # Costruisco una griglia regolare di nr x nc punti
    # =========================================================================

    nr, nc = dem.shape

    # =========================================================================
    # Calcolo della distanza media tra i diversi pluviometri
    # =========================================================================

    dp = np.zeros(len(posx), dtype=float)

    for jj in range(len(posx)):

        for ii in range(len(posx)):
            dp[jj] = (
                    dp[jj]
                    + np.sqrt(
                (posx[jj] - posx[ii]) ** 2
                + (posy[jj] - posy[ii]) ** 2
            )
            )

    # =========================================================================
    # Codice KERNEL
    # =========================================================================

    W = np.zeros(
        dem.shape,
        dtype=float,
    )

    for i in range(nr):

        for j in range(nc):

            # =============================================================
            # MATLAB utilizza indici che partono da 1.
            # Manteniamo quindi gli stessi valori numerici utilizzati
            # nel codice originale.
            # =============================================================

            i_matlab = i + 1
            j_matlab = j + 1

            conta = 0

            N = 0.0

            if dem[i, j] > 0.0:

                for ii in range(len(posx)):

                    d = max(
                        0.0,
                        np.sqrt(
                            (j_matlab - posx[ii]) ** 2
                            + (i_matlab - posy[ii]) ** 2
                        ),
                    )

                    if d < R:
                        N = (
                                N
                                + (
                                        (R ** 2 - d ** 2)
                                        / (R ** 2 + d ** 2)
                                )
                        )

                        conta = len(posx)

                if conta > 0:
                    W[i, j] = (
                            W[i, j]
                            + N / conta
                    )


            else:

                W[i, j] = np.nan

    return W


def regression_function(b, x):
    """
    Equivalente MATLAB di:
    yfcn = @(b,x) ...
        b(1).*(x(:,1).^2) + ...
        b(2).*x(:,2) + ...
        b(3).*x(:,3) + ...
        b(4);
    """
    return (
            b[0] * (x[:, 0] ** 2)
            + b[1] * x[:, 1]
            + b[2] * x[:, 2]
            + b[3]
    )
