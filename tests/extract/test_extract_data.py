# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 09:58:53

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import datatest as dt
import pandas as pd
import pytest
from os.path import exists
from glob import glob

from src.extract.extract_data import *
from src.commons.paths import *


# functions that return data tables
# to be used in succeeding tests
def df_store():
    return get_data_table(urls["stores_dataset"])

def df_sales():
    return get_data_table(urls["sales_dataset"])

def df_features():
    return get_data_table(urls["features_dataset"])


# tests statements
def test_exceptions_are_working():
    invalid_url = "www.google.com"
    with pytest.raises(Exception):
        get_data_table("invalid_url")

@pytest.mark.skip
def test_write_data_table():
    write_data_table(df_store().head(), "test")
    data_file = f"{raw_data_dir}/test.csv"
    assert exists(data_file)


# def test_column_names():
# 	assert list(df().columns) == ["Store", "Type", "Size"]

def test_column_names_are_uppercase():
    dt.validate(df_store().columns, lambda x: x.istitle())

@pytest.mark.mandatory
def test_column_names():
    required_column_names = {
        df_store: {"Store", "Type", "Size"},
        df_sales: {"Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"},
        df_features: {"Fuel_Price", "Date", "Unemployment", "MarkDown4", 
            "Store", "MarkDown2", "MarkDown3", "CPI", "MarkDown1", "Temperature", "MarkDown5", "IsHoliday"},
    }
    for data_table in required_column_names:
        dt.validate(data_table().columns, required_column_names[data_table])

def test_extracted_data_present():
    global data_files
    data_files = (
        f"{raw_data_dir}/stores_dataset.csv",
        f"{raw_data_dir}/sales_dataset.csv",
        f"{raw_data_dir}/features_dataset.csv",
    )
    
    for data_file in data_files:
        assert os.path.exists(data_file)

    # no other file present in folder
    assert set(glob(f"{raw_data_dir}/*")) == set(data_files)


def test_all_data_written():
    # row counts correspond to expected
    row_counts = {
        "stores" : 46,
        "sales" : 1001,
        "features" : 8191,
    }
    for d, data_file in enumerate(data_files):
        with open(data_file) as dfile:
            assert row_counts[list(row_counts.keys())[d]] == len(dfile.readlines())