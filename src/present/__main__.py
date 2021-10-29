# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-29 10:27:46
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 18:13:17

import os
import sys
from glob import glob

import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import clean_data_dir, present_data_dir
from ingest.ingest_data import write_spark_table

from present.present_data import (
    generate_sales_department_data, 
    generate_ds_department_data
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


from dataclasses import dataclass



if not os.path.exists(present_data_dir):
    os.mkdir(present_data_dir)


@dataclass
class Data:
	stores: pd.DataFrame
	sales:pd.DataFrame
	features: pd.DataFrame


data = Data(
	stores = spark.read.load(f"{clean_data_dir}/stores"),
	sales = spark.read.load(f"{clean_data_dir}/sales"),
	features = spark.read.load(f"{clean_data_dir}/features"),
)


print("writing presentation table for sales department ...")
write_spark_table(
    data = generate_sales_department_data(
        stores_table = data.stores,
        sales_table = data.sales
        ),
    partition_col = "p_ingest_date",
    output_dir = present_data_dir,
    name = "sales_dept",
    mode = "append"
)


print("writing presentation table for data science department ...")
write_spark_table(
    data = generate_ds_department_data(
        stores_table = data.stores,
        sales_table = data.sales,
        features_table = data.features,
        ),
    partition_col = "p_ingest_date",
    output_dir = present_data_dir,
    name = "ds_dept",
    mode = "append"
)
