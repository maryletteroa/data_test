# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 13:49:46
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 18:58:12

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _includes.paths import clean_data_dir, expectations_suite_dir, clean_data_profile_dir
from _profile._profile_data import (
    build_expectation_suite_from_spark,
    generate_data_profile_from_spark,
    spark)

from glob import glob
from sys import argv

if not os.path.exists(clean_data_profile_dir):
    os.mkdir(clean_data_profile_dir)

for table in glob(f"{clean_data_dir}/*"):
    name = os.path.basename(table)
    data = spark.read.load(table)


    print(f"building profile for clean table: {name} ...")
    generate_data_profile_from_spark(
        data = data,
        output_dir = clean_data_profile_dir ,
        prefix = name
    )


    if "--expect" in argv:
        print(f"building expectation suite for clean table: {name} ...")
        build_expectation_suite_from_spark(
            data = data,
            expectations_path = f"{expectations_suite_dir}/clean_{name}_expectation_suite.json"
        )
