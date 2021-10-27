# -*- coding: utf-8 -*-

"""Summary
Functions to generate the data profiles and
Great Expecations suites
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:12
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-27 18:39:29

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext
from great_expectations.dataset import SparkDFDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
import spark_df_profiling
from pyspark.sql import SparkSession


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
        ProfileReport: A ProfileReport obect
    
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

def build_expectation_suite_from_pandas_profiling(
    pandas_profile: ProfileReport,
    data_context: DataContext,
    suite_name: str,
    )-> None:
    """Summary
    
    Builds GE expectations suite from pandas profiling

    Args:
        pandas_profile (ProfileReport): the pandas_profiling object
        data_context (DataContext): GE data context object
        suite_name (str): Suite name
    """
    pandas_profile.to_expectation_suite(
        suite_name=suite_name,
        data_context=data_context,
        run_validation=False,
        build_data_docs=True,
        )

def build_expectation_suite_from_spark(
    data: pd.DataFrame,
    expectations_path: str
    ) -> None:
    """Summary
    
    Generations expectation suits from Spark dataframe

    Args:
        data (DataFrame): A Spark dataframe object
        expectations_path (str): Where the expectation suite json file will be written
    """
    profiler = BasicDatasetProfiler()

    expectation_suite, validation_result = \
        BasicDatasetProfiler.profile(
            data_asset=SparkDFDataset(data), 
        )

    with open(expectations_path, "w") as outf:
        print(expectation_suite, file=outf)

def generate_data_profile_from_spark(
    data: pd.DataFrame,
    output_dir: str,
    prefix: str
    ) -> None:
    """
    Generates profile reports from Spark tables
    
    Args:
        data (pd.DataFrame): Spark Dataframe
        input_dir (str): Path to Spark tables (parquet)
        output_dir (str): Output directory of HTML report
        prefix (str): Prefix of HTML file
    """
    profile = spark_df_profiling.ProfileReport(data)
    profile.to_file(outputfile=f"{output_dir}/{prefix}_data_profile_report.html")