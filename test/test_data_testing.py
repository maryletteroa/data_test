# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 17:39:17
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 19:13:17

from context import *
from data_testing.commons import *

def test_data_testing():
    data_files = (
        f"{root_dir}/data_testing/raw/store_dataset.csv",
        f"{root_dir}/data_testing/raw/sales_dataset.csv",
        f"{root_dir}/data_testing/raw/features_dataset.csv",
        )
    for data_file in data_files:
        assert os.path.exists(data_file)