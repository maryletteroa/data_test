# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-29 10:27:33
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 12:11:01


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date
)

spark = SparkSession.builder.getOrCreate()


# sales with stores

def generate_sales_department_data(
    stores_table: pd.DataFrame,
    sales_table: pd.DataFrame,
    ) -> pd.DataFrame:

    
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
    stores_table: pd.DataFrame,
    sales_table: pd.DataFrame,
    features_table: pd.DataFrame,
    ) -> pd.DataFrame:


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