# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:25
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 09:09:59

"""
Functions to ingest data to raw
"""


import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from pyspark.sql import SparkSession
import pyspark.sql.dataframe 
import pandas as pd
from pyspark.sql.functions import (
    current_timestamp, 
    current_date, 
    lit)
from typing import IO

spark = SparkSession.builder.getOrCreate()

def read_csv_to_spark(
        csv_file_path:str,
        status: str,
        tag: str,
        ) -> pd.DataFrame:
    """Reads the csv from source to spark dataframe
    
    Args:
        csv_file_path (str): Path to csv file
        status (str): status of the data [new]
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Description
    """
    df = spark.read \
        .option("header", True) \
        .csv(csv_file_path)
    df = df \
        .withColumn("status", lit(status)) \
        .withColumn("tag", lit(tag)) \
        .withColumn("p_ingest_date", current_date()) \
        .withColumn("ingest_datetime", current_timestamp())

    return df

# data checks before writing

def write_delta_table(
        df: pd.DataFrame,
        partition_col: str,
        output_dir: str,
        mode: str = "append",
    ) -> None:

    """Writes data to delta table
    
    Args:
        df (pd.DataFrame): Spark dataframe to write
        partition_col (str): column to parition
        output_dir (str): Output path
        mode (str, optional): Mode (default="append")
    
    Returns:
        None: Writes delta table to output_dir
    """
    return (
        df \
            .write \
            .format("delta") \
            .mode(mode) \
            .partitionBy(partition_col)
            .parquet(output_dir)
    )

# path is not empty
# data types are strings
# p_ingest_date and ingest_datetime cols are present
# expected number of colums 
# expected number of rows
# no duplicate imports