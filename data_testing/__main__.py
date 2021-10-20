# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 16:53:56
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 19:15:28

from extract_data import *
try:
    from commons import *
except ModuleNotFoundError:
    from .commons import *


for url in urls:
	print(f"getting data for: {url}...")
	write_data_table(
		get_data_table(urls[url]),
		url
	)
