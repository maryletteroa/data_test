# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 19:20:42

"""
Function to generate the data profile
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext


from datetime import datetime


def generate_data_profile(
    df: pd.DataFrame, 
    title: str, 
    output_dir: str,
    prefix: str) -> ProfileReport:
    
    """Outputs data pandas data profile from csv
    
    Args:
        df (pd.DataFrame): Pandas dataframe to generate profile from
        title (str): Name of file to go in the report title
        output_dir (str): Root output directory for reports
        prefix (str): Name of report file
    
    Returns:
        ProfileReport: A ProfileReport obect
    
    Deleted Parameters:
        name (str): Description
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
    
    profile = ProfileReport(
        df=df,
        title=f"{title} Data Profile Report",
        minimal=False,
        sensitive=False,
        dataset = metadata,
        explorative=True,
    )
    profile.to_file(f"{output_dir}/{prefix}_data_profile_report.html")

    return profile

def build_expectation_suite(
    pandas_profile: ProfileReport,
    data_context: DataContext,
    suite_name: str,
    ):

    pandas_profile.to_expectation_suite(
        suite_name=suite_name,
        data_context=data_context,
        run_validation=True,
        build_data_docs=True,
        )
