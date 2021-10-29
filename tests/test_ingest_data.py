# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 14:06:00

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from glob import glob
from src._includes.paths import data_urls, raw_data_dir
from src.extract.extract_from_urls import write_table_csv, get_data_from_urls
from src.ingest.ingest_data import *

import pytest
import datatest as dt
from pyspark.sql import SparkSession
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from collections import namedtuple

from pyspark.sql.types import (
    _parse_datatype_string, 
    StringType, 
    DataType
)
spark = SparkSession.builder.getOrCreate()

# ---- start fixtures ------#
@pytest.fixture
def test_csv(tmpdir):
    write_table_csv(
        get_data_from_urls(data_urls["stores"]).head(), 
        tmpdir, 
        "test"
    )
    return f"{tmpdir}/test.csv"

@pytest.fixture
def tables():
    return (
        f"{raw_data_dir}/stores",
        f"{raw_data_dir}/sales",
        f"{raw_data_dir}/features"
    )

# ---- end fixtures ------#



def test_read_csv_to_spark(test_csv):
    df = read_csv_to_spark(
            csv_file_path = test_csv,
            tag = "raw",
            )
    assert all(
            col for col in ["p_ingest_date", "tag", "ingest_datetime"] 
            if col in df.columns
        )

def test_write_spark_table(tmpdir, test_csv):
    df = read_csv_to_spark(
            csv_file_path = test_csv,
            tag = "raw",
    )

    write_spark_table(
        data=df,
        partition_col = "p_ingest_date",
        output_dir = tmpdir,
        name= "test")
    assert os.path.exists(f"{tmpdir}/test")

# ------- data test ----------

@pytest.mark.skipif(
    glob(f"{raw_data_dir}/*") == [],
    reason="The data have not been ingested",
)
class TestData:
    def test_raw_tables_present(self, tables):

        for table in tables:
            assert os.path.exists(table)

        # no other file present in folder
        assert set(glob(f"{raw_data_dir}/*")) == set(tables)
