# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 08:08:19
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 17:00:35

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _includes.paths import raw_data_dir, expectations_suite_dir, raw_data_profile_dir
from _profile.profile_data import (
    build_expectation_suite_from_spark,
    generate_data_profile_from_spark,
    spark)
from glob import glob

if not os.path.exists(raw_data_profile_dir):
    os.mkdir(raw_data_profile_dir)



for table in glob(f"{raw_data_dir}/*"):
    name = os.path.basename(table)
    data = spark.read.load(table)
    if glob(f"{raw_data_profile_dir}/{name}*") == []:

        print(f"building profile for raw table: {name} ...")
        generate_data_profile_from_spark(
            data = data,
            output_dir = raw_data_profile_dir ,
            prefix = name
        )
    else:
        print(f"target files already present!")

    if glob(f"{expectations_suite_dir}/raw_{name}*") == []:
        print(f"building expectation suite for raw table: {name} ...")
        build_expectation_suite_from_spark(
            data = data,
            expectations_path = f"{expectations_suite_dir}/raw_{name}_expectation_suite.json"
    )
    else:
        print(f"target files already present!")