import numpy as np

def _updating_evaluate_Quality(a2dData):

    # =========================================================================
    # Descrizione
    # =========================================================================
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
