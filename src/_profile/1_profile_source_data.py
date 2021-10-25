# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:24
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 17:00:28

"""
Generates profiles of pertinent datasets
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from glob import glob
import pandas as pd

from _includes.paths import (
    source_data_dir, 
    source_data_profile_dir, 
    great_expectations_root
)
from _profile._profile_data import generate_data_profile, build_expectation_suite_from_pandas_profiling
from great_expectations.data_context import DataContext

# Profile source data

if not os.path.exists(source_data_profile_dir):
    os.mkdir(source_data_profile_dir)

for csv in glob(f"{source_data_dir}/*.csv"):
    basename = os.path.basename(csv)
    name = os.path.splitext(basename)[0]
    title = name.title()
    profile = generate_data_profile(
        df=pd.read_csv(csv),
        title=title,
        output_dir=f"{source_data_profile_dir}",
        prefix=name,
    )
    build_expectation_suite_from_pandas_profiling(
        pandas_profile = profile,
        data_context = DataContext(
              context_root_dir=great_expectations_root
        ),
        suite_name = f"source_{name}_expectation_suite"
    )
