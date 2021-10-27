# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-25 09:38:06
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-27 21:34:10

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import raw_data_dir, transformed_data_dir
from ingest.ingest_data import write_spark_table

from transform.transform_data import (
    transform_stores, 
    transform_sales,
    transform_features,
    tag_negative_sales,
    spark
)


from glob import glob

if not os.path.exists(transformed_data_dir):
    os.mkdir(transformed_data_dir)


write_spark_table(
        data = transform_stores(
            path=f"{raw_data_dir}/stores",
            tag="transformed"
        ),
        partition_col = "p_ingest_date",
        output_dir = transformed_data_dir,
        name = "stores",
        mode = "append",
    )


write_spark_table(
        data = transform_features(
            path=f"{raw_data_dir}/features",
            tag="transformed"
        ),
        partition_col = "p_ingest_date",
        output_dir = transformed_data_dir,
        name = "features",
        mode = "append",
    )


#----------- with health check ----- #

sales_tables = tag_negative_sales(
    data = transform_sales(
        path=f"{raw_data_dir}/sales",
        tag="transformed"
    ),
    tag = "quarantined",
    )

write_spark_table(
        data = sales_tables.good,
        partition_col = "p_ingest_date",
        output_dir = transformed_data_dir,
        name = "sales",
        mode = "append",
    )

write_spark_table(
        data = sales_tables.quarantined,
        partition_col = "p_ingest_date",
        output_dir = transformed_data_dir,
        name = "sales",
        mode = "append",
    )
