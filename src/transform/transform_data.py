# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 09:37:54
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 20:10:51



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
    lit)

spark = SparkSession.builder.getOrCreate()


def transform_stores(
        path: str,
        status: str,
        tag: str,
    ) -> pd.DataFrame:
    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("size", col("size").cast("double")) \
        .withColumn("status", lit(status)) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())
    return df


def transform_sales(
    path: str,
    status: str,
    tag: str
    ) -> pd.DataFrame:

    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("store", col("store").cast("int")) \
        .withColumn("dept", col("dept").cast("int")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("weekly_sales", col("weekly_sales").cast("double")) \
        .drop("isholiday") \
        .withColumn("status", lit(status)) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())


    return df

def transform_features(
    path: str,
    status:str,
    tag: str
    ) -> pd.DataFrame:

    df = spark.read.load(path)
    df = df \
        .select([col(c).alias(c.lower()) for c in df.columns]) \
        .withColumn("fuel_price", col("fuel_price").cast("decimal")) \
        .withColumn("date", col("date").cast("date")) \
        .withColumn("unemployment", col("unemployment").cast("decimal")) \
        .drop("cpi", "markdown1", "markdown2", "markdown3", "markdown4", "markdown5") \
        .withColumn("temperature", col("temperature").cast("decimal")) \
        .withColumnRenamed("isholiday", "is_holiday") \
        .withColumn("is_holiday", col("is_holiday").cast("boolean")) \
        .withColumn("status", lit(status)) \
        .withColumn("tag", lit(tag)) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df
