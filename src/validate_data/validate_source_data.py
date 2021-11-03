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
    source_data_dir
)
from _validate_data import validate_csv_file

context = great_expectations.data_context.DataContext(
    context_root_dir=great_expectations_root
    )


files = os.listdir(source_data_dir)

for file in files:
    basename = os.path.basename(file)
    name = basename.split(".")[0]
    validate_csv_file(
        context = context,
        expectation_suite_name = f"source_{name}_expectation_suite",
        path_to_csv = f"{source_data_dir}/{basename}",
        datasource_name = "source_dir",
        data_asset_name = basename
    )


