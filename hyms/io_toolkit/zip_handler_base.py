
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import os
from copy import deepcopy

from hyms.default.lib_default_generic import zip_extension

from hyms.io_toolkit.lib_io_gzip import (unzip_file_name as unzip_file_name_gzip,
                                         zip_file_name as zip_file_name_gzip)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
class ZipHandler:

    type_class = 'zip_base'
    type_data_compress = {'gzip': zip_file_name_gzip}
    type_data_uncompress = {'gzip': unzip_file_name_gzip}

    def __init__(self, file_name_compress: str, file_name_uncompress: str = None,
                 zip_extension: str = '.gz') -> None:

        self.file_name_compress = file_name_compress
        self.zip_extension = check_zip_extension(zip_extension)

        if file_name_uncompress is None:
            file_name_uncompress = remove_zip_extension(file_name_compress, zip_extension_template=zip_extension)
        self.file_name_uncompress = file_name_uncompress

        if self.zip_extension.lower() in ['gz', '.gz']:
            self.zip_format = 'gzip'
        else:
            raise ValueError(f'Format {self.zip_extension} not supported.')

        self.fx_compress = self.type_data_compress.get(self.zip_format, self.zip_error)
        self.fx_uncompress = self.type_data_uncompress.get(self.zip_format, self.zip_error)

        if self.zip_extension in self.file_name_compress:
            self.zip_check = True
        else:
            self.zip_check = False

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to uncompress file
    def uncompress_file_name(self):
        if self.zip_extension in self.file_name_compress:
            return self.fx_uncompress(self.file_name_compress, self.file_name_uncompress)
        else:
            return None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to compress file
    def compress_file_name(self):
        if self.zip_extension in self.file_name_compress:
            return self.fx_compress(self.file_name_uncompress, self.file_name_compress)
        else:
            return None
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    def zip_error(self):
        """
        Error data.
        """
        raise NotImplementedError
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to check if zip extension is a known string
def check_zip_extension(zip_extension, zip_extension_expected=None):

    if zip_extension_expected is None:
        zip_extension_expected = ['gz']

    # Check if string starts with point
    if zip_extension.startswith('.'):
        zip_extension_parser = zip_extension[1:]
    else:
        zip_extension_parser = zip_extension

    # Check if zip extension is a known string
    if zip_extension_parser not in zip_extension_expected:
        raise RuntimeError('Zip extension is wrong.')

    return zip_extension_parser

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to add only compressed extension
def add_zip_extension(file_name_unzip, zip_extension_template=zip_extension):

    if not zip_extension_template[0] == '.':
        zip_extension_template = '.' + zip_extension_template

    if not file_name_unzip.endswith(zip_extension_template):
        file_name_zip = file_name_unzip + zip_extension_template
    else:
        file_name_zip = file_name_unzip
    return file_name_zip
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to remove only compressed extension
def remove_zip_extension(file_name_zip, file_path_tmp=None, zip_extension_template=zip_extension):

    # Check zip extension format
    if zip_extension is not None:
        # Check zip extension format in selected mode
        zip_ext_str = check_zip_extension(zip_extension_template)
    else:
        # Check zip extension format in constants mode
        file_name_unzip, zip_extension_template = os.path.splitext(file_name_zip)
        # Check zip extension format
        zip_ext_str = check_zip_extension(zip_extension_template)

    if not zip_ext_str.startswith('.'):
        zip_ext_str = '.' + zip_ext_str

    file_path_defined = file_name_zip.split(zip_ext_str)[0]

    if file_path_tmp is not None:

        if not os.path.exists(file_path_tmp):
            os.makedirs(file_path_tmp, exist_ok=True)

        file_folder_defined, file_name_defined = os.path.split(file_path_defined)
        file_name_unzip = os.path.join(file_path_tmp, file_name_defined)
    else:
        file_name_unzip = deepcopy(file_path_defined)

    return file_name_unzip
# ----------------------------------------------------------------------------------------------------------------------
