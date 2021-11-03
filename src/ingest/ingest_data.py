# -*- coding: utf-8 -*-
"""
Functions to ingest and tag data, and write
the data as parquet files

"""
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:25
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-11-03 08:48:51


import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import (
    current_date,
    current_timestamp, 
    lit)
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame


spark = SparkSession.builder.getOrCreate()

def read_csv_to_spark(
        csv_file_path:str,
        tag: str,
        ) -> pd.DataFrame:
    """
    Reads the csv from source to spark dataframe
    
    Args:
        csv_file_path (str): Path to csv file
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Description
    
    Deleted Parameters:
        spark (SparkSession): Description
    """
    df = spark.read \
        .option("header", True) \
        .csv(csv_file_path)
    df = df \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df

# data checks before writing

def write_spark_table(
        data: pyspark_DataFrame,
        partition_col: str,
        output_dir: str,
        name: str,
        mode: str = "append",
    ) -> None:

    """Writes data to delta table
    
    Args:
        data (pyspark_DataFrame): Spark dataframe
        partition_col (str): Column name where table will be partitioned
        output_dir (str): Output directory
        name (str): Name of table
        mode (str, optional): Mode (default="append")
    
    Returns:
        None: Writes delta table to output_dir
    
    """
    return (
        data \
            .write \
            .format("parquet") \
            .mode(mode) \
            .partitionBy(partition_col)
            .parquet(f"{output_dir}/{name}")
    )