from typing import Dict
from shybox.runner_toolkit.namelist.lib_utils_dataclass import M, D, Var

# namelist template for S3M version 5.3.3
namelist_s3m_533: Dict[str, Dict[str, Var]] = {

    "S3M_Snow": {
        "a1dArctUp": D([1.1, 1.1, 1.1, 1.1]),
        "a1dAltRange": D([1500, 2000, 2500]),
        "iGlacierValue": D(1),
        "dRhoSnowFresh": D(200),
        "dRhoSnowMax": D(400),
        "dRhoSnowMin": D(67.9),
        "dSnowQualityThr": D(0.3),
        "dMeltingTRef": D(1),
        "dIceMeltingCoeff": D(1),
        "iSWEassInfluence": D(6),
        "dWeightSWEass": D(0.25),
        "dRefreezingSc": D(1.0),
        "dModFactorRadS": D(1.125),
        "sWYstart": D("09"),
        "dDebrisThreshold": D(0.2),
        "iDaysAvgTSuppressMelt": D(10),
    },

    "S3M_Namelist": {
        "sDomainName": M(),

        "iFlagDebugSet": D(0),
        "iFlagDebugLevel": D(3),

        "iFlagTypeData_Forcing_Gridded": D(3),
        "iFlagTypeData_Updating_Gridded": D(3),
        "iFlagTypeData_Ass_SWE_Gridded": D(3),

        "iFlagRestart": M(),
        "iFlagSnowAssim": M(),
        "iFlagSnowAssim_SWE": D(0),
        "iFlagIceMassBalance": D(0),
        "iFlagThickFromTerrData": D(0),
        "iFlagGlacierDebris": D(1),
        "iFlagOutputMode": D(1),
        "iFlagAssOnlyPos": D(0),

        "a1dGeoForcing": M(),
        "a1dResForcing": M(),
        "a1iDimsForcing": M(),

        "iSimLength": M(),
        "iDtModel": M(),

        "iDtData_Forcing": M(),
        "iDtData_Updating": M(),
        "iDtData_Output": M(),
        "iDtData_AssSWE": M(),

        "iScaleFactor_Forcing": D(10),
        "iScaleFactor_Update": D(100),
        "iScaleFactor_SWEass": D(10),

        "sTimeStart": M(),
        "sTimeRestart": M(),

        "sPathData_Static_Gridded": M(),
        "sPathData_Forcing_Gridded": M(),
        "sPathData_Updating_Gridded": M(),
        "sPathData_Output_Gridded": M(),
        "sPathData_Restart_Gridded": M(),
        "sPathData_SWE_Assimilation_Gridded": M(),
    },

    "S3M_Constants": {
        "dRhoW": D(1000),
    },

    "S3M_Command": {
        "sCommandZipFile": D("gzip -f filenameunzip > LogZip.txt"),
        "sCommandUnzipFile": D("gunzip -c filenamezip > filenameunzip"),
        "sCommandRemoveFile": D("rm filename"),
        "sCommandCreateFolder": D("mkdir -p path"),
    },

    "S3M_Info": {
        "sReleaseVersion": D("5.3.3"),
        "sAuthorNames": D("Avanzi F., Gabellani S., Delogu F., Silvestro F."),
        "sReleaseDate": D("2024/11/13"),
    },
}
