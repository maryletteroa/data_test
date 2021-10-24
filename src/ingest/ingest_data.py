# -*- coding: utf-8 -*-
"""Summary

Attributes:
    spark (TYPE): Description
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:25
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 13:09:19

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


def get_data_table(url: str) -> pd.DataFrame:
    """Parses table out from a website
    
    Args:
        url (str): Website URL
    
    Returns:
        pd.DataFrame: A pandas dataframe containing the data
    
    Raises:
        Exception: Catch all for all errors including invalid or unavailable URLs.
    """
    try:
        table: pd.DataFrame = pd.read_html(url, header=1,)[0].iloc[:, 1:]
    except:
        raise Exception("Something went wrong")
    return table


def create_spark_dataframe(
        data: pd.DataFrame,
        status: str,
        tag: str,
        ) -> pd.DataFrame:
    """Summary
    
    Args:
        data (pd.DataFrame): Description
        status (str): Description
        tag (str): Description
    
    Returns:
        pd.DataFrame: Description
    """
    df = spark.createDataFrame(
        data = data)

    df = df \
        .withColumn("status", lit(status)) \
        .withColumn("tag", lit(tag)) \
        .withColumn("p_ingest_date", current_date()) \
        .withColumn("ingest_datetime", current_timestamp())

    return df

def write_delta_table(
        data: pd.DataFrame,
        output_dir: str,
        name: str,
        mode: str = "append",
        partition_col: str = "p_ingest_date",
    ) -> None:

    """Writes data to delta table
    
    Args:
        data (pd.DataFrame): Spark dataframe to write
        output_dir (str): Output path
        name (str): Name of table
        mode (str, optional): Mode (default="append")
        partition_col (str, optional): column to parition
    
    Returns:
        None: Writes delta table to output_dir
    """
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    return (
        data \
            .write \
            .format("delta") \
            .mode(mode) \
            .partitionBy(partition_col)
            .parquet(f"{output_dir}/{name}")
    )

# path is not empty
# data types are strings
# p_ingest_date and ingest_datetime cols are present
# expected number of colums 
# expected number of rows
# no duplicate imports