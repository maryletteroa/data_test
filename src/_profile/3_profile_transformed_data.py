# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 13:49:46
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-27 22:23:59

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _includes.paths import transformed_data_dir, expectations_suite_dir, transformed_data_profile_dir
from _profile._profile_data import (
    build_expectation_suite_from_spark,
    generate_data_profile_from_spark,
    spark)
from glob import glob

if not os.path.exists(transformed_data_profile_dir):
    os.mkdir(transformed_data_profile_dir)

for table in glob(f"{transformed_data_dir}/*"):
    name = os.path.basename(table)
    data = spark.read.load(table)
    if glob(f"{transformed_data_profile_dir}/{name}*") == []:

        print(f"building profile for transformed table: {name} ...")
        generate_data_profile_from_spark(
            data = data,
            output_dir = transformed_data_profile_dir ,
            prefix = name
        )
    else:
        print(f"target files already present!")

    if glob(f"{expectations_suite_dir}/transformed_{name}*") == []:
        print(f"building expectation suite for transformed table: {name} ...")
        build_expectation_suite_from_spark(
            data = data,
            expectations_path = f"{expectations_suite_dir}/transformed_{name}_expectation_suite.json"
    )
    else:
        print(f"target files already present!")