# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 09:50:10
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 13:52:31

"""
Extracts retail data from websites
And writes them into csv files
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from commons.paths import raw_data_dir
import pandas as pd
from typing import IO


def get_data_table(url: str) -> pd.DataFrame:
    """Parses table out from a website

    Args:
        url (str): Website URL

    Returns:
        pd.DataFrame: A pandas dataframe containing the data

    Raises:
        Exception: Catch all for all errors including invalid or unavailable URLs.
    """
    try:
        table: pd.DataFrame = pd.read_html(url, header=1,)[0].iloc[:, 1:]
    except:
        raise Exception("Something went wrong")
    return table


def write_data_table(table: pd.DataFrame, 
        output_dir: str,
        prefix: str,
        ) -> IO:

    """Writes table to a csv file
    
    Args:
        table (pd.DataFrame): Table or data to be written
        output_dir (str): Output directory
        prefix (str): Name of file
    
    Returns:
        IO: [description]: A csv file

    """
    return table.to_csv(f"{output_dir}/{prefix}.csv", index=False)