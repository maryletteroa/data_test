# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 16:53:56
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 17:30:47

from extract_data import *


urls = {
    "store_dataset" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales_dataset": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQS1tAlWgq16nxeyQ6tIsyfifyWc5u_2kxV5L7Z4Z6hnueKhimkc0OkbF6Ug9_1mgS48-jTiH8-wz1A/pubhtml",
    "features_dataset":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"
}


for url in urls:
	print(f"getting data for: {url}...")
	write_data_table(
		get_data_table(urls[url]),
		url
	)
