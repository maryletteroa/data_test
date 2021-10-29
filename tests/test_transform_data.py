# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 19:19:49
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 16:29:30

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src._includes.paths import raw_data_dir

import pytest

from src.transform.transform_data import (
    transform_stores,
    transform_sales,
    transform_features,
    tag_negative_sales,
    negative_sales_to_null,
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
from pyspark.sql import Row

# ---- start fixtures ------#

@pytest.fixture
def table():
    df = tag_negative_sales(
        data = transform_sales(
            path = f"{raw_data_dir}/sales",
            tag = "good",
            ),
        tag = "quarantined"
    )

    return df

# ---- end fixtures ------#



def test_transform_stores():
    df = transform_stores(
        path = f"{raw_data_dir}/stores",
        tag = "test"
    )

    assert df.schema == StructType(fields = [
        StructField("store", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("tag", StringType(), False),
        StructField("ingest_datetime", TimestampType(), False),
        StructField("p_ingest_date", DateType(), False),
    ])


def test_transform_sales():
    df = transform_sales(
        path = f"{raw_data_dir}/sales",
        tag = "test",
    )

    assert df.schema == StructType(fields = [
        StructField("store", IntegerType(), True),
        StructField("dept", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("weekly_sales", FloatType(), True),
        StructField("tag", StringType(), False),
        StructField("ingest_datetime", TimestampType(), False),
        StructField("p_ingest_date", DateType(), False),
    ])

def test_transform_features():
    df = transform_features(
        path = f"{raw_data_dir}/features",
        tag = "test",
    )

    assert df.schema == StructType(fields = [
        StructField("store", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("temperature", FloatType(), True),
        StructField("fuel_price", FloatType(), True),
        StructField("markdown", FloatType(), True),
        StructField("cpi", FloatType(), True),
        StructField("unemployment", FloatType(), True),
        StructField("is_holiday", BooleanType(), True),
        StructField("tag", StringType(), False),
        StructField("ingest_datetime", TimestampType(), False),
        StructField("p_ingest_date", DateType(), False),
    ])

def test_tag_negative_sales():
    df = tag_negative_sales(
        data = transform_sales(
            path = f"{raw_data_dir}/sales",
            tag = "good",
            ),
        tag = "quarantined"
    )

    assert df.good.select("tag").distinct().collect() == [Row(tag="good")]
    assert df.quarantined.select("tag").distinct().collect() == [Row(tag="quarantined")]

def test_negative_sales_to_null(table):
    df = negative_sales_to_null(
        data = table.quarantined
    )

    assert df.select("weekly_sales").distinct().collect() == [Row(weekly_sales=None)]