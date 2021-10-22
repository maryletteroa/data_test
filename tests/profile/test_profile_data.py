# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-22 10:20:16

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import datatest as dt
import pytest
from os.path import exists
from glob import glob

from src.profile.profile_data import *
from src.commons.paths import raw_data_profile_dir

# TODO: add pytest evaluation of lengths of test
@pytest.mark.skipif(
    glob(f"{raw_data_profile_dir}/*") == [],
    reason="The profiles have not been extracted yet",
)
def test_profile_data_present():
    data_files = (
        f"{raw_data_profile_dir}/raw_features_profile_report.html",
        f"{raw_data_profile_dir}/raw_sales_profile_report.html",
        f"{raw_data_profile_dir}/raw_stores_profile_report.html",
    )
    
    for data_file in data_files:
        assert os.path.exists(data_file)

    # no other file present in folder
    assert set(glob(f"{raw_data_profile_dir}/*")) == set(data_files)

