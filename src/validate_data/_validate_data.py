# -*- coding: utf-8 -*-
"""
Functions to validate data against GE expectation suites
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-30 15:22:55
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-11-04 23:00:04


import datetime
import great_expectations
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import great_expectations_root


def validate_csv_file(
    context: great_expectations.data_context.DataContext,
    expectation_suite_name: str,
    path_to_csv: str,
    datasource_name: str,
    data_asset_name: str,
    ) -> None:
    """
    
    Function run validation of csv files against GE expectation suites
    
    Args:
        context (great_expectations.data_context.DataContext): GE context
        expectation_suite_name (str): Expectation suite name
        path_to_csv (str): Path to csv file
        datasource_name (str): Datasource name as registered in GE
        data_asset_name (str): Data asset name
    """
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    batch_kwargs = {
        "path": path_to_csv,
        "datasource": datasource_name,
        "reader_method": "read_csv",
        "data_asset_name": data_asset_name,
    }
    batch = context.get_batch(batch_kwargs, suite)


    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
              "batch_kwargs": batch_kwargs,
              "expectation_suite_names": [expectation_suite_name]
            }
        ]
    ).run()
    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context.build_data_docs()


def validate_spark_table(
    context: great_expectations.data_context.DataContext,
    expectation_suite_name: str,
    path_to_spark_table: str,
    datasource_name: str,
    data_asset_name: str,
    ) -> None:


    """
    
    Function run validation of spark table against GE expectation suites
    
    Args:
        context (great_expectations.data_context.DataContext): GE context
        expectation_suite_name (str): Expectation suite name
        path_to_spark_table (str): Path to csv file
        datasource_name (str): Datasource name as registered in GE
        data_asset_name (str): Data asset name
    
    """
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    batch_kwargs = {
        "path": path_to_spark_table,
        "datasource": datasource_name,
        "reader_method": "parquet",
        "data_asset_name": data_asset_name,
    }

    batch = context.get_batch(batch_kwargs, suite)


    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
              "batch_kwargs": batch_kwargs,
              "expectation_suite_names": [expectation_suite_name]
            }
        ]
    ).run()
    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context.build_data_docs()
