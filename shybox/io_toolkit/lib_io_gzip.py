"""
Library Features:

Name:          lib_io_zip
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240810'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import gzip
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to unzip file
def unzip_file_name(file_name_compress: str, file_name_uncompress : str) -> bool:

    file_handle_compress = gzip.GzipFile(file_name_compress, "rb")
    file_handle_uncompress = open(file_name_uncompress, "wb")

    file_data_compress = file_handle_compress.read()
    file_handle_uncompress.write(file_data_compress)

    file_handle_compress.close()
    file_handle_uncompress.close()

    return True

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to zip file
def zip_file_name(file_name_uncompress : str, file_name_compress : str) -> bool:

    file_handle_uncompress = open(file_name_uncompress, 'rb')
    file_handle_compress = gzip.open(file_name_compress, 'wb')

    file_handle_compress.writelines(file_handle_uncompress)

    file_handle_compress.close()
    file_handle_uncompress.close()

    return True

# ----------------------------------------------------------------------------------------------------------------------
