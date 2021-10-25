# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:11
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 09:52:37

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import raw_data_dir, source_data_dir
from ingest_data import read_csv_to_spark, write_delta_table


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

names = ("stores", "sales", "features")
# ingest raw data
if not os.path.exists(raw_data_dir):
    os.mkdir(raw_data_dir)

for name in names:
    df = read_csv_to_spark(
        spark = spark,
        csv_file_path = f"{source_data_dir}/{name}.csv",
        status = "new",
        tag = "raw"
        )

    write_delta_table(
        df = df,
        partition_col = "p_ingest_date",
        output_dir = raw_data_dir,
        prefix = name,
        mode = "append",
        )