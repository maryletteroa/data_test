# -*- coding: utf-8 -*-
"""Summary
Functions to transform raw  table
"""

# @Author: Marylette B. Roa
# @Date:   2021-10-25 09:37:54
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-27 22:20:35



import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, 
    current_date, 
    current_timestamp,
    col,
    lit,
    to_date,
    date_format,
)

from dataclasses import dataclass


spark = SparkSession.builder.getOrCreate()


def transform_stores(
        path: str,
        tag: str,
    ) -> pd.DataFrame:
    """Summary
    Transform stores table
    
    Args:
        path (str): Path to stores raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Transformed and tagged spark table
    """
    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("size", col("size").cast("int")) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())
    return df


def transform_sales(
    path: str,
    tag: str
    ) -> pd.DataFrame:
    """Summary
    Transform stores table
    
    Args:
        path (str): Path to sales raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Transformed and tagged spark table
    """
    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("dept", col("dept").cast("int")) \
        .withColumn("date", to_date(col("date"), "dd/mm/yyyy")) \
        .withColumn("date", date_format(col("date"), "yyyy-MM-dd")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("weekly_sales", col("weekly_sales").cast("float")) \
        .drop("isholiday") \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df

def transform_features(
    path: str,
    tag: str
    ) -> pd.DataFrame:
    """Summary
    Transform stores table
    
    Args:
        path (str): Path to features raw table
        tag (str): tag for the data e.g. raw, processed
    
    Returns:
        pd.DataFrame: Transformed and tagged spark table
    """
    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("fuel_price", col("fuel_price").cast("float")) \
        .withColumn("date", to_date(col("date"), "dd/mm/yyyy")) \
        .withColumn("date", date_format(col("date"), "yyyy-MM-dd")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("unemployment", col("unemployment").cast("float")) \
        .withColumn("temperature", col("temperature").cast("float")) \
        .withColumn("markdown", col("markdown").cast("float")) \
        .withColumn("cpi", col("cpi").cast("float")) \
        .withColumnRenamed("isholiday", "is_holiday") \
        .withColumn("is_holiday", col("is_holiday").cast("boolean")) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df


# ------------- health check ------------ #


@dataclass
class Sales:

    """Summary
    Contains the partitioned Sales data
    good and quarantined
    """
    
    good: pd.DataFrame
    quarantined: pd.DataFrame



def tag_negative_sales(
    data: pd.DataFrame,
    tag: str,
    ) -> Sales:
    """Summary
    
    Args:
        data (pd.DataFrame): Spark dataframe containing sales table
        tag (str): Tag for quarantined table
    
    Returns:
        Sales: Namedtuple containing the partitioned good and quarantined sales table
    """
    # split data into good and quarantine
    df_quarantine = data.filter(data.weekly_sales < 0)
    df_quarantine = df_quarantine \
        .withColumn("tag", lit(tag)) \


    df_good = data.filter(data.weekly_sales >= 0)


    return Sales(
        good = df_good,
        quarantined= df_quarantine,
        )
