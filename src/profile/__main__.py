# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:24
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-22 10:40:59

"""
Generates profiles of pertinent datasets
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from commons.paths import source_data_dir, source_data_profile_dir
from profile_data import generate_data_profile_from_csv


# Profile raw data pre-ingestion
raw_data_csvs = {
    "Source Features Data Profile": (f"{source_data_dir}/features_dataset.csv", 
        f"{source_data_profile_dir}/source_features_profile_report.html",
        {"Store": str,},
    ),
    "Source Sales Data Profile": (f"{source_data_dir}/sales_dataset.csv",
        f"{source_data_profile_dir}/source_sales_profile_report.html",
        {"Store": str, "Dept": str,}, 
    ),
    "Source Stores Data Profile": (f"{source_data_dir}/stores_dataset.csv",
        f"{source_data_profile_dir}/source_stores_profile_report.html",
        {"Store": str,}, 
    ),
}


for name,details in raw_data_csvs.items():
    generate_data_profile_from_csv(
        csv_path=details[0],
        dtype=details[2],
        title=name,
        output_path=details[1],
)
