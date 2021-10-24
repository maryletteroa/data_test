# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 14:44:11
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 13:03:46

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from includes.paths import production_data, raw_data_dir
from ingest_data import get_data_table, create_spark_dataframe, write_delta_table

extracted_df = {}

# extract data table from url
for name in production_data:
    data_url = production_data[name]
    print(f"getting data for: {name}...")

    extracted_df[name] = get_data_table(data_url)

# ingest to delta table
for name, df_extracted, in extracted_df.items():
    df = create_spark_dataframe(
        data=path,
        status="new",
        tag="raw"
        )

    write_delta_table(
        df = df,
        partition_col = "p_ingest_date",
        output_dir = f"{raw_data_dir}/{name}",
        mode = "append",
        )