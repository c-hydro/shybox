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
import shutil
import os
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# define compress filename
def define_compress_filename(file_name_uncompress: str,
                             remove_ext: bool = False, uncompress_ext: str = '.nc', compress_ext: str ='.gz') -> str:
    if remove_ext:
        file_name_compress = file_name_uncompress.replace(uncompress_ext, uncompress_ext)
    else:
        file_name_compress = file_name_uncompress + compress_ext
    return file_name_compress
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Function to compress a file to .gz and remove the uncompressed file
def uncompress_and_remove_SD(input_file, output_file: str = None, remove_original: bool = True):

    # if output_file is not provided, use the input_file name with .gz extension
    if output_file is None:
        output_file = input_file + '.gz'

    # compress the file
    with open(input_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # remove the uncompressed file
    if remove_original:
        os.remove(input_file)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to uncompress a file to .gz and remove the uncompressed file
def uncompress_and_remove(input_file: str, output_file: str = None, remove_original: bool = False):

    # check if the file has a compression extension
    compression_extensions = ['.gz']
    _, ext = os.path.splitext(input_file)

    if ext not in compression_extensions:
        print(f"The file '{input_file}' does not have a compression extension.")
        return

    if output_file is None:
        output_file = input_file[:-3]

    # Uncompress the file based on its extension
    if ext == '.gz':
        with gzip.open(input_file, 'rb') as f_in:
            with open(output_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        print(f"Unsupported compression format: {ext}")
        return

    # Remove the compressed file
    if remove_original:
        os.remove(input_file)
        print(f"The file '{input_file}' has been uncompressed and removed.")

    return output_file
# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to compress a file to .gz and remove the uncompressed file
def compress_and_remove(input_file, output_file: str = None, remove_original: bool = True):

    # if output_file is not provided, use the input_file name with .gz extension
    if output_file is None:
        output_file = input_file + '.gz'

    # compress the file
    with open(input_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # remove the uncompressed file
    if remove_original:
        os.remove(input_file)

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to unzip file
def unzip_file_name(file_name_compress: str, file_name_uncompress: str) -> bool:

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
def zip_file_name(file_name_uncompress: str, file_name_compress: str) -> bool:

    file_handle_uncompress = open(file_name_uncompress, 'rb')
    file_handle_compress = gzip.open(file_name_compress, 'wb')

    file_handle_compress.writelines(file_handle_uncompress)

    file_handle_compress.close()
    file_handle_uncompress.close()

    return True

# ----------------------------------------------------------------------------------------------------------------------
