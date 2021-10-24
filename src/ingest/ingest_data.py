# -*- coding: utf-8 -*-
"""Summary

Attributes:
    spark (TYPE): Description
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:25
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 16:16:36

"""
Functions to ingest and tag data to raw delta tables
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
        spark: SparkSession,
        csv_file_path:str,
        status: str,
        tag: str,
        ) -> pd.DataFrame:
    """Reads the csv from source to spark dataframe
    
    Args:
        spark (SparkSession): Description
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
        prefix: str,
        mode: str = "append",
    ) -> None:

    """Writes data to delta table
    
    Args:
        df (pd.DataFrame): Spark dataframe to write
        partition_col (str): column to parition
        output_dir (str): Output path
        prefix (str): Prefix/ name of delta table
        mode (str, optional): Mode (default="append")
    
    Returns:
        None: Writes delta table to output_dir
    """
    return (
        df \
            .write \
            .format("parquet") \
            .mode(mode) \
            .partitionBy(partition_col)
            .parquet(f"{output_dir}/{prefix}")
    )