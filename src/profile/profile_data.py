# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 12:15:52

"""
Generates a data profile
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
from typing import Optional


def generate_data_profile_report(
    csv_path: str, 
    dtype: Optional[dict],
    description: str, 
    output_path: str) -> None:

    metadata = {
        "description": "This is a sample profiling report.", 
        "copyright_holder": "Someone",
        "copyright_year": "Some year",
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
            title=f"{description} Data Profile",
            minimal=False,
            sensitive=False,
            dataset = metadata,
        ).to_file(f"{output_path}")
    )