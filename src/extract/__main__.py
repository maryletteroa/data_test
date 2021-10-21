# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 16:53:56
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 16:04:19

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from commons.paths import production_data, source_data_dir
from extract_data import write_data_table, get_data_table

for name in production_data:
    print(f"getting data for: {name}...")
    write_data_table(
        get_data_table(production_data[name]),
        f"{source_data_dir}",
        name
    )
