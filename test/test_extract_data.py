# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 19:03:35

import context

import datatest as dt
import pandas as pd
import pytest
from os.path import exists

from data_testing.extract_data import *
from data_testing.commons import *

urls = {
    "store_dataset" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales_dataset": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQuEhSASCf9qSpODvARVhzsnxxL3S20IbtrovVZfjKm8YEmP88ZU5gHXdw8GEAR_g7vAtOKWEvR8QYT/pubhtml",
    "features_dataset":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml",
    "invalid_url": "google.com",
}


def df_store():
    return get_data_table(urls["store_dataset"])

def df_sales():
    return get_data_table(urls["sales_dataset"])

def df_features():
    return get_data_table(urls["features_dataset"])

def test_exceptions_are_working():
    with pytest.raises(Exception):
        get_data_table(urls["invalid_url"])

# def test_column_names():
# 	assert list(df().columns) == ["Store", "Type", "Size"]

def test_column_names_are_uppercase():
    dt.validate(df_store().columns, lambda x: x.istitle())

@pytest.mark.mandatory
def test_column_names_store():
    required_column_names = {
        df_store: {"Store", "Type", "Size"},
        df_sales: {"Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"},
        df_features: {"Fuel_Price", "Date", "Unemployment", "MarkDown4", "Store", "MarkDown2", "MarkDown3", "CPI", "MarkDown1", "Temperature", "MarkDown5", "IsHoliday"},
    }
    for data_table in required_column_names:
        dt.validate(data_table().columns, required_column_names[data_table])

@pytest.mark.skip
def test_write_data_table():
    write_data_table(df_store().head(), "test")
    data_file = f"{root_dir}/data_testing/raw/test.csv"
    assert exists(data_file)

# TODO: test end of file
# TODO: group test cases