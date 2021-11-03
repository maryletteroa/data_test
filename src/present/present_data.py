# -*- coding: utf-8 -*-
"""
Functions to prepare data for consumption of the
sales and data science (ds) department
"""
# @Author: Marylette B. Roa
# @Date:   2021-10-29 10:27:33
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-11-03 08:48:11


import pandas as pd
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date
)
from pyspark.sql.dataframe import DataFrame as pyspark_DataFrame


def generate_sales_department_data(
    stores_table: pyspark_DataFrame,
    sales_table: pyspark_DataFrame,
    ) -> pyspark_DataFrame:
    """
    Args:
        stores_table (pyspark_DataFrame): Spark table containing stores data
        sales_table (pyspark_DataFrame): Spark table containing sales data
    
    Returns:
        pyspark_DataFrame: Spark table for sales department
    """
    df_stores = stores_table \
        .select("store", "type")

    df_sales = sales_table \
        .select("store", "dept", "date", "weekly_sales")

    df = df_sales \
        .join(
            df_stores,
            on=["store"],
            how="left"
        ) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())

    return df


# features with sales with stores
def generate_ds_department_data(
    stores_table: pyspark_DataFrame,
    sales_table: pyspark_DataFrame,
    features_table: pyspark_DataFrame,
    ) -> pyspark_DataFrame:
    """
    Args:
        stores_table (pyspark_DataFrame): Spark table containing stores data
        sales_table (pyspark_DataFrame): Spark table containing sales data
        features_table (pyspark_DataFrame): Spark table containing features data
    
    Returns:
        pyspark_DataFrame: Spark table for data science department
    """
    df_stores = stores_table \
        .select("store", "type", "size")

    df_sales = sales_table \
        .select("store", "dept", "weekly_sales")


    df_features = features_table \
        .select("store", "date", "temperature", 
            "fuel_price", "markdown", "cpi", 
            "unemployment", "is_holiday"
        )

    df = df_sales \
        .join(
            df_stores,
            on=["store"],
            how="left"
        ) \
        .join(
            df_features,
            on=["store"],
            how="left"
        ) \
        .withColumn("ingest_datetime", current_timestamp()) \
        .withColumn("p_ingest_date", current_date())


    return df