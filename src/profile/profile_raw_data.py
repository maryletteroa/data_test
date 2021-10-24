# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-21 10:02:24
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-24 14:05:44

"""
Generates profiles of pertinent datasets
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from includes.paths import raw_data_profile_dir, data_urls
from ingest.ingest_data import get_data_table
from commons.profile_data import generate_data_profile


# Profile raw data pre-ingestion
extracted_df = {}
for name, url in data_urls.items():
    print(f"getting data for: {name}...")

    extracted_df[name] = get_data_table(url)


for name, df in extracted_df.items():
    generate_data_profile(
        df=df,
        title=name.title(),
        output_dir=f"{raw_data_profile_dir}",
        name=name,
)
