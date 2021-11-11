# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-11-11 09:17:51
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-11-11 09:26:48

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from src.extract.extract_from_urls import get_data_from_urls, write_table_csv
from src._includes.paths import data_urls

@pytest.fixture(scope="session")
def store_data(tmpdir_factory):
    df = get_data_from_urls(data_urls["stores"])
    temp_path = str(tmpdir_factory.mktemp("test_dir"))
    write_table_csv(df.head(), temp_path, "test")
    return f"{temp_path}/test.csv"