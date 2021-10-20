# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 17:39:17
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 20:16:51

from context import *
from data_testing.commons import *
from glob import glob

def test_extracted_data_present():
    global data_files
    data_files = (
        f"{root_dir}/data_testing/raw/stores_dataset.csv",
        f"{root_dir}/data_testing/raw/sales_dataset.csv",
        f"{root_dir}/data_testing/raw/features_dataset.csv",
    )
    
    for data_file in data_files:
        assert os.path.exists(data_file)

    # no other file present in folder
    assert set(glob(f"{root_dir}/data_testing/raw/*")) == set(data_files)

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