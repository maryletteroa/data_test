# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 16:53:56
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 09:03:12

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from commons.paths import urls
from extract_data import write_data_table, get_data_table

for url in urls:
	print(f"getting data for: {url}...")
	write_data_table(
		get_data_table(urls[url]),
		url
	)
