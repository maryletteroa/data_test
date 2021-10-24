# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 13:27:06

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


def generate_data_profile(
    df: pd.DataFrame, 
    title: str, 
    output_dir: str,
    name: str) -> IO:
    
    """Outputs data pandas data profile from csv
    
    Args:
        data (pd.DataFrame): Pandas dataframe to profile
        dtype (Optional[dict]): Data type to override pandas type coercion
        title (str): Name of file to go in the report title
        output_path (str): Where the report will be written
        name (str): Description
    
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

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    return(
        ProfileReport(
            df=df,
            title=f"{title} Data Profile Report",
            minimal=False,
            sensitive=False,
            dataset = metadata,
            explorative=True,
        ).to_file(f"{output_dir}/{name}_data_profile_report.html")
    )


def generate_ge_suit():
    suite = profile.to_expectation_suite()