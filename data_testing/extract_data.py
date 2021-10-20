# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 09:50:10
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 18:11:39

"""
Extracts retail data from websites
And writes them into csv files
"""

from typing import IO
import os
import context
from .commons import *
import pandas as pd


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
    out_path: str = f"{root_dir}/data_testing/raw"
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    return table.to_csv(f"{out_path}/{file_name}.csv", index=False)