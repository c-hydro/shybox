"""
Library Features:

Name:          lib_default_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261020'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# definition of wkt for projections
crs_wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,' \
           'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],' \
           'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
crs_epsg = 'EPSG:4326'

# definition of crs attrs
crs_epsg_code = 4326
crs_grid_mapping_name = "latitude_longitude"
crs_longitude_of_prime_meridian = 0.0
crs_semi_major_axis = 6378137.0
crs_inverse_flattening = 298.257223563

# definition of crs object
crs_obj = {
    'crs_epsg_code': crs_epsg_code,
    'crs_epsg_string': crs_epsg,
    'crs_wkt': crs_wkt,
    'crs_grid_mapping_name': crs_grid_mapping_name,
    'crs_longitude_of_prime_meridian': crs_longitude_of_prime_meridian,
    'crs_semi_major_axis': crs_semi_major_axis,
    'crs_inverse_flattening': crs_inverse_flattening
}
# ----------------------------------------------------------------------------------------------------------------------
