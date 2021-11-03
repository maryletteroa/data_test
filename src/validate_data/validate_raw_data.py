# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-30 18:00:44
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-30 18:08:01

import datetime
import great_expectations
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from _includes.paths import (
    great_expectations_root, 
    raw_data_dir
)
from _validate_data import validate_spark_table

context = great_expectations.data_context.DataContext(
    context_root_dir=great_expectations_root
    )

files = os.listdir(raw_data_dir)

for file in files:
    name = os.path.basename(file)
    validate_spark_table(
        context = context,
        expectation_suite_name = f"raw_{name}_expectation_suite",
        path_to_spark_table = f"{raw_data_dir}/{name}",
        datasource_name = "raw_dir",
        data_asset_name = f"raw_{name}"
    )

