# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:11
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 17:24:37

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from commons.paths import source_data_dir, raw_data_dir
from ingest_data import read_csv_to_spark, write_delta_table

# from pyspark.sql.types import (
#     StructType, 
#     StructField, 
#     StringType, 
#     IntegerType, 
#     DateType, 
#     DoubleType,
#     BooleanType,
#     )


# ingest src data to raw delta table
source_data_sets = {
    "store": f"{source_data_dir}/stores_dataset.csv",
    "sales": f"{source_data_dir}/sales_dataset.csv",
    "features": f"{source_data_dir}/features_dataset.csv",
}


for name, path, in source_data_sets.items():
    df = read_csv_to_spark(
        csv_file_path=path,
        status="new",
        tag="raw"
        )

    write_delta_table(
        df = df,
        partition_col = "p_ingest_date",
        output_dir = f"{raw_data_dir}/{name}",
        mode = "append",
        )