# -*- coding: utf-8 -*-

"""Summary
Function to retrive data from url
and write them to csv

"""
# @Author: Marylette B. Roa
# @Date:   2021-10-24 14:26:09
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 17:15:55

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from typing import IO


def get_data_from_urls(url: str) -> pd.DataFrame:
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


def write_table_csv(table: pd.DataFrame, 
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