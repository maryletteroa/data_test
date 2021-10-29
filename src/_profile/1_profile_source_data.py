# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:24
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 17:03:04

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
    great_expectations_root,
    expectations_suite_dir
)
from _profile.profile_data import generate_data_profile_from_csv, build_expectation_suite_from_pandas_profiling
from great_expectations.data_context import DataContext


if not os.path.exists(source_data_profile_dir):
    os.mkdir(source_data_profile_dir)


for csv in glob(f"{source_data_dir}/*.csv"):
    basename = os.path.basename(csv)
    name = os.path.splitext(basename)[0]
    title = name.title()

    if glob(f"{source_data_profile_dir}/{name}*") ==[]:

        profile = generate_data_profile_from_csv(
            df=pd.read_csv(csv),
            title=title,
            output_dir=f"{source_data_profile_dir}",
            prefix=name,
        )
    else:
        print(f"{source_data_profile_dir} not empty")

    if glob(f"{expectations_suite_dir}/source_{name}*") == []:
        build_expectation_suite_from_pandas_profiling(
            pandas_profile = profile,
            data_context = DataContext(
                  context_root_dir=great_expectations_root
            ),
            suite_name = f"source_{name}_expectation_suite"
        )
    else:
        print(f"target files already present!")