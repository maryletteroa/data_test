# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-24 14:27:58
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 15:05:37

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import datatest as dt
import pandas as pd
import pytest
from glob import glob
from collections import namedtuple

from src.extract.extract_from_urls import *
from src._includes.paths import test_data_urls, source_data_dir

# functions that return data tables
# to be used in succeeding tests


@pytest.fixture
def csvs():
    return (
            f"{source_data_dir}/stores.csv",
            f"{source_data_dir}/sales.csv",
            f"{source_data_dir}/features.csv",
    )

@pytest.fixture
def datasets(csvs):
    Data = namedtuple("Data", ("stores", "sales", "features"))
    datasets = data = Data(
            stores = pd.read_csv(csvs[0]),
            sales = pd.read_csv(csvs[1]),
            features = pd.read_csv(csvs[2])
            )
    return datasets


def test_exceptions_are_working():
    invalid_url = "www.google.com"
    with pytest.raises(Exception):
        get_data_from_urls("invalid_url")

def test_write_table_csv(tmpdir):
    df = get_data_from_urls(test_data_urls["stores"])
    write_table_csv(df.head(), tmpdir, "test")
    data_file = f"{tmpdir}/test.csv"
    assert os.path.exists(data_file)

@pytest.mark.skipif(
    glob(f"{source_data_dir}/*") == [],
    reason="The production data has not been extracted yet",
)
def test_source_data_dir_exists():
    assert os.path.exists(source_data_dir)

def test_extracted_data(csvs):

    # extracted data exists in directory
    for csv in csvs:
        assert os.path.exists(csv)

    # no other file present in folder
    assert set(glob(f"{source_data_dir}/*")) == set(csvs)


# ---------- data testing----------------


# @pytest.mark.skipif(
#     glob(f"{source_data_dir}/*") == [],
#     reason="The production data has not been extracted yet",
# )

def test_shapes(datasets):
    shapes = {
        "stores" : (45, 3),
        "sales" : (421570, 5),
        "features" : (8190, 12),
    }

    for name, shape in shapes.items():
        assert getattr(datasets, name).shape == shape

def test_column_names(datasets):
    required_column_names = {
        "stores": {"Store", "Type", "Size"},
        "sales": {"Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"},
        "features": {"Fuel_Price", "Date", "Unemployment", "MarkDown4", 
            "Store", "MarkDown2", "MarkDown3", "CPI", "MarkDown1", "Temperature", "MarkDown5", "IsHoliday"},
    }

    for name, cols in required_column_names.items():
        dt.validate(getattr(datasets, name).columns, cols)