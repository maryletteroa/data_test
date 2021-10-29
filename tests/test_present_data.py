# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-29 12:16:08
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 16:53:36

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src._includes.paths import clean_data_dir
import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from src.presentation.present_data import (
    generate_sales_department_data,
    generate_ds_department_data
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DateType,
    BooleanType,
    FloatType,
)

def test_generate_sales_department_data():
    df = generate_sales_department_data(
        stores_table = spark.read.load(f"{clean_data_dir}/stores"),
        sales_table = spark.read.load(f"{clean_data_dir}/sales")
    )

    assert df.schema == StructType(fields = [
        StructField("store", IntegerType(), True),
        StructField("dept", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("weekly_sales", FloatType(), True),
        StructField("type", StringType(), True),
        StructField("ingest_datetime", TimestampType(), False),
        StructField("p_ingest_date", DateType(), False),
    ])

def test_generate_ds_department_data():
    df = generate_ds_department_data(
        stores_table = spark.read.load(f"{clean_data_dir}/stores"),
        sales_table = spark.read.load(f"{clean_data_dir}/sales"),
        features_table = spark.read.load(f"{clean_data_dir}/features")
    )

    assert df.schema == StructType(fields = [
        StructField("store", IntegerType(), True),
        StructField("dept", IntegerType(), True),
        StructField("weekly_sales", FloatType(), True),
        StructField("type", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("temperature", FloatType(), True),
        StructField("fuel_price", FloatType(), True),
        StructField("markdown", FloatType(), True),
        StructField("cpi", FloatType(), True),
        StructField("unemployment", FloatType(), True),
        StructField("is_holiday", BooleanType(), True),
        StructField("ingest_datetime", TimestampType(), False),
        StructField("p_ingest_date", DateType(), False),
    ])