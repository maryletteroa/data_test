# -*- coding: utf-8 -*-
"""
Functions to build Great Expectations suite
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-30 09:35:27
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-30 11:09:01

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas_profiling import ProfileReport
from great_expectations.data_context import DataContext
from great_expectations.dataset import SparkDFDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame

def build_expectation_suite_from_pandas_profiling(
    pandas_profile: ProfileReport,
    data_context: DataContext,
    suite_name: str,
    )-> None:
    """
    
    Builds GE expectations suite from pandas profiling
    
    Args:
        pandas_profile (ProfileReport): the pandas_profiling object
        data_context (DataContext): GE data context object
        suite_name (str): Suite name
    
    """
    return pandas_profile.to_expectation_suite(
        suite_name=suite_name,
        data_context=data_context,
        run_validation=False,
        build_data_docs=True,
    )


def build_expectation_suite_from_spark(
    data: pyspark_DataFrame,
    expectations_path: str
    ) -> None:
    """
    
    Generations expectation suits from Spark dataframe
    
    Args:
        data (pyspark_DataFrame): A Spark dataframe object
        expectations_path (str): Where the expectation suite json file will be written
    """
    
    expectation_suite, validation_result = \
        BasicDatasetProfiler.profile(
            data_asset=SparkDFDataset(data), 
        )

    with open(expectations_path, "w") as outf:
        print(expectation_suite, file=outf)
