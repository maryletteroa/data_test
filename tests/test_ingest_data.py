# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 14:07:37

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from glob import glob
from collections import namedtuple

import datatest as dt
import pytest
import pandas as pd

from src.includes.paths import raw_data_dir, test_data_urls
from src.ingest.ingest_data import *


@pytest.fixture
def datasets():
    Data = namedtuple("Data", ("stores","sales","features"))
    data = Data(
        stores = get_data_table(test_data_urls["stores"]),
        sales = get_data_table(test_data_urls["sales"]),
        features = get_data_table(test_data_urls["features"]),
        )
    return data

@pytest.mark.mandatory
def test_column_names(datasets):

    cols = (
           (datasets.stores, {"Store", "Type", "Size"}),
           (datasets.sales, {"Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"}),
           (datasets.features, {"Fuel_Price", "Date", "Unemployment", "MarkDown4", 
                "Store", "MarkDown2", "MarkDown3", "CPI", "MarkDown1", "Temperature", "MarkDown5", "IsHoliday"}),
    )

    for cols in [*cols]:
        df, required_cols = cols
        dt.validate(df.columns, required_cols )

@pytest.mark.skip
def test_column_names_are_uppercase(datasets):
    for name in datasets._fields:
        dt.validate(getattr(datasets, name).columns, lambda x: x.istitle())


def test_expected_shape(datasets):
    shapes = (
       (datasets.stores, (45, 3)),
       # (datasets.sales, (421570, 5)),
       (datasets.sales, (1000, 5)),
       (datasets.features, (8190, 12)),
    )

    for shape in [*shapes]:
        df, expected_shape = shape
        assert df.shape == expected_shape

def test_exceptions_are_working():
    invalid_url = "www.google.com"
    with pytest.raises(Exception):
        get_data_table("invalid_url")

def test_create_spark_dataframe(datasets):
    for name in datasets._fields:
        df = create_spark_dataframe(
            data=getattr(datasets, name),
            status="new",
            tag="raw"
            )
        assert all(col for col in ["status", "p_ingest_date", "ingest_datetime", "tag"] if col in df.columns)

def test_write_delta_table(datasets):
    for name in datasets._fields:
        df = create_spark_dataframe(
            data = getattr(datasets, name),
            status = "new",
            tag = "raw"
            )

        write_delta_table(
            data=df,
            output_dir = raw_data_dir,
            name= name)
        assert os.path.exists(f"{raw_data_dir}/{name}")

@pytest.mark.skipif(
    glob(f"{raw_data_dir}/*") == [],
    reason="The data has not been ingested yet",
)
def test_raw_data_present():
    data_files = (
        f"{raw_data_dir}/stores",
        f"{raw_data_dir}/sales",
        f"{raw_data_dir}/features",
    )
    for data_file in data_files:
        assert os.path.exists(data_file)

    # no other file present in folder
    assert set(glob(f"{raw_data_dir}/*")) == set(data_files)


# path is not empty
# data types are strings
# p_ingest_date and ingest_datetime cols are present
# expected number of colums 
# expected number of rows
# no duplicate imports