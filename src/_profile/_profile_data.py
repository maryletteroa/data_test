# -*- coding: utf-8 -*-

"""
Functions to generate the data profiles and
Great Expecations suites

Attributes:
    spark (TYPE): Description
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-11-03 08:47:30

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext
from great_expectations.dataset import SparkDFDataset
import spark_df_profiling
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame

from datetime import datetime

spark = SparkSession.builder.getOrCreate()


def generate_data_profile_from_csv(
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
        ProfileReport: A ProfileReport object
    
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

def generate_data_profile_from_spark(
    data: pyspark_DataFrame,
    output_dir: str,
    prefix: str
    ) -> None:
    """
    Generates profile reports from Spark tables
    
    Args:
        data (pyspark_DataFrame): Spark Dataframe
        output_dir (str): Output directory of HTML report
        prefix (str): Prefix of HTML file
    
    """
    
    profile = spark_df_profiling.ProfileReport(data)
    profile.to_file(outputfile=f"{output_dir}/{prefix}_data_profile_report.html")
