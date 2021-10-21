# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 09:50:10
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 09:01:38

"""
Extracts retail data from websites
And writes them into csv files
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from commons.paths import root_dir


import pandas as pd
from typing import IO


def get_data_table(url: str) -> pd.DataFrame:
    """Parses table out from a website

    Args:
        url (str): Website URL

    Returns:
        pd.DataFrame: A pandas dataframe containing the data

    Raises:
        Exception: When the URL is not valid
    """
    try:
        table = pd.read_html(url, header=1,)[
            0
        ].iloc[:, 1:]
    except:
        raise Exception("URL is invalid")
    return table


def write_data_table(table: pd.DataFrame, file_name: str) -> IO:
    """Writes table to a csv file

    Args:
        table (pd.DataFrame): Table or data to be written
        file_name (str): File name of csv file

    Returns:
        IO: [description]: A csv file
    """
    out_path: str = f"{root_dir}/data/raw"
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    return table.to_csv(f"{out_path}/{file_name}.csv", index=False)