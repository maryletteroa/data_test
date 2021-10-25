# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 09:38:06
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 14:35:37

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import raw_data_dir, raw_data_dir
from transform.transform_data import (
    transform_stores, 
    transform_sales,
    transform_features
)
from glob import glob

transform_stores(
    path=f"{raw_data_dir}/stores",
    status="processed",
    tag="good"
    )

transform_sales(
    path=f"{raw_data_dir}/sales",
    status="processed",
    tag="good"
    )


transform_features(
    path=f"{raw_data_dir}/features",
    status="processed",
    tag="good"
    )