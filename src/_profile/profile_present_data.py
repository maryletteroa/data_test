# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-29 17:56:58
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 18:58:18


# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 13:49:46
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 17:00:39

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _includes.paths import present_data_dir, expectations_suite_dir, present_data_profile_dir
from _profile._profile_data import (
    build_expectation_suite_from_spark,
    generate_data_profile_from_spark,
    spark)

from glob import glob
from sys import argv

if not os.path.exists(present_data_profile_dir):
    os.mkdir(present_data_profile_dir)

for table in glob(f"{present_data_dir}/*"):
    name = os.path.basename(table)
    data = spark.read.load(table)

    print(f"building profile for presentation table: {name} ...")
    generate_data_profile_from_spark(
        data = data,
        output_dir = present_data_profile_dir ,
        prefix = name
    )

    if "--expect" in argv:
        print(f"building expectation suite for presentation table: {name} ...")
        build_expectation_suite_from_spark(
            data = data,
            expectations_path = f"{expectations_suite_dir}/present_{name}_expectation_suite.json"
        )
