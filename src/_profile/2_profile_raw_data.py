# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 08:08:19
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 09:06:47

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from _includes.paths import raw_data_dir, expectations_suite_dir
from _profile._profile_data import build_expectation_suite_from_spark
from pyspark.sql import SparkSession
from glob import glob

spark = SparkSession.builder.getOrCreate()

for data in glob(f"{raw_data_dir}/*"):
    name = os.path.basename(data)
    print(f"building expecation suite for raw table: {name} ...")
    build_expectation_suite_from_spark(
        data = spark.read.option("header",True).load(data),
        expectations_path = f"{expectations_suite_dir}/raw_{name}_expectation_suite.json"
    )