# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 17:57:22

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from glob import glob
from src._includes.paths import test_data_urls, raw_data_dir
from src.extract.extract_from_urls import write_table_csv, get_data_from_urls
from src.ingest.ingest_data import *

import pytest
import datatest as dt
from pyspark.sql import SparkSession
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

from pyspark.sql.types import (
    _parse_datatype_string, 
    StringType, 
    DataType
)
spark = SparkSession.builder.getOrCreate()

@pytest.fixture
def test_csv(tmpdir):
    write_table_csv(
        get_data_from_urls(test_data_urls["stores"]).head(), 
        tmpdir, 
        "test"
    )
    return f"{tmpdir}/test.csv"


def test_read_csv_to_spark(test_csv):
    df = read_csv_to_spark(
            spark = spark,
            csv_file_path = test_csv,
            status = "new",
            tag = "raw",
            )
    assert all(
            col for col in ["status", "p_ingest_date", "ingest_datetime", "tag"] 
            if col in df.columns
        )

def test_write_delta_table(tmpdir, test_csv):
    df = read_csv_to_spark(
            spark = spark,
            csv_file_path = test_csv,
            status = "new",
            tag = "raw",
            )

    write_delta_table(
        df=df,
        partition_col = "p_ingest_date",
        output_dir = tmpdir,
        prefix= "test")
    assert os.path.exists(f"{tmpdir}/test")



@pytest.mark.skipif(
    glob(f"{raw_data_dir}/*") == [],
    reason="The data have not been ingested",
)
@pytest.mark.mandatory
def test_raw_tables_present():
    data_files = (
        f"{raw_data_dir}/stores",
        f"{raw_data_dir}/sales",
        f"{raw_data_dir}/features",
    )
    
    for data_file in data_files:
        assert os.path.exists(data_file)

    # no other file present in folder
    assert set(glob(f"{raw_data_dir}/*")) == set(data_files)

@pytest.fixture
def df_stores():
    return spark.read.load(f"{raw_data_dir}/stores")

def test_raw_table_schema(df_stores):
    raw_data_schema = _parse_datatype_string("""
        Store STRING,
        Type STRING,
        Size STRING,
        status STRING,
        tag STRING,
        ingest_datetime TIMESTAMP,
        p_ingest_date DATE"""
    )
 
    assert df_stores.schema == raw_data_schema


def test_raw_table_schema2(df_stores):
    test = ge.dataset.SparkDFDataset(df_stores)
    assert test.expect_column_values_to_be_of_type("Store", "StringType").success

def test_raw_table_shape(df_stores):
    assert (df_stores.count(), len(df_stores.columns)) == (45,7)


def test_raw_store_column(df_stores):
    df = df_stores.toPandas()
    assert df.drop_duplicates().shape == (45, 7)
