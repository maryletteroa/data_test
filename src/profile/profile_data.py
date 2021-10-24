# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-22 10:40:40

"""
Function to generate the data profile
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
import great_expectations as ge

from typing import Optional, IO
from datetime import datetime


def generate_data_profile_from_csv(
    csv_path: str, 
    dtype: Optional[dict], # not sure how to properly type dict(str, type)
    title: str, 
    output_path: str) -> IO:
    
    """Outputs data pandas data profile from csv
    
    Args:
        csv_path (str): Path of csv file
        dtype (Optional[dict]): Data type to override pandas type coercion
        title (str): Name of file to go in the report title
        output_path (str): Where the report will be written
    
    Returns:
        IO: Writes an HTML report
    """
    metadata = {
        "description": "This is a sample profiling report.", 
        "creator": "maryletteroa",
        "author": "maryletteroa",
        "copyright_year": datetime.now().year,
        "url": "www.example.com",
    }

    # >> also column descriptions
    # >> customize profile views


    return(
        ProfileReport(
            pd.read_csv(
                csv_path,
                dtype=dtype
            ), 
            title=f"{title}",
            minimal=False,
            sensitive=False,
            dataset = metadata,
            explorative=True,
        ).to_file(f"{output_path}")
    )


def generate_ge_suit():
    suite = profile.to_expectation_suite()