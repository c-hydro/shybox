# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read array data
def get_file_array(file_name: str, columns_name: list = None, index_name: str = 'index'):
    file_data = pd.read_table(file_name, header=None, delim_whitespace=True)
    if columns_name is None:
        columns_name = ['index', 'values']
    file_data.columns = columns_name
    return file_data
# ----------------------------------------------------------------------------------------------------------------------
