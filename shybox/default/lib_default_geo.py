"""
Library Features:

Name:          lib_default_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240808'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# definition of wkt for projections
crs_wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,' \
           'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],' \
           'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
crs_epsg = 'EPSG:4326'

# definition of crs
crs_epsg_code = 4326
crs_grid_mapping_name = "latitude_longitude"
crs_longitude_of_prime_meridian = 0.0
crs_semi_major_axis = 6378137.0
crs_inverse_flattening = 298.257223563
# ----------------------------------------------------------------------------------------------------------------------
