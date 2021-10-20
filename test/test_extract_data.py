# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 11:43:02

import context
import datatest as dt
import pandas as pd
import pytest

from data_testing.extract_data import *

urls = {
    "valid_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "invalid_url": "google.com",
}


def df():
    return get_data_table(urls["valid_url"])


def test_exceptions_are_working():
    get_data_table(urls["valid_url"])


# def test_column_names():
# 	assert list(df().columns) == ["Store", "Type", "Size"]

# using datatest
def test_column_names():
    required_column_names = {"Store", "Type", "Size"}
    dt.validate(df().columns, required_column_names)


def test_column_names_are_uppercase():
    dt.validate(df().columns, lambda x: x.istitle())
