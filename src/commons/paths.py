# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 17:14:05
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-21 18:02:06

"""
Contains shared variables
"""

# higher level directories
root_dir = "/mnt/d/Projects/data_tests_demo/retail_data"
scripts_dir = f"{root_dir}/src"
docs_dir = f"{root_dir}/docs"

# data dirs
data_dir = f"{root_dir}/data"
source_data_dir = f"{data_dir}/source"
raw_data_dir = f"{data_dir}/raw"

# profile dirs
data_profile_dir = f"{docs_dir}/data_profiles"
raw_data_profile_dir = f"{data_profile_dir}/raw"


test_data = {
    "stores_dataset" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales_dataset": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTCEpWPXGMM_kEg_mElJbUQk_7yFfHeEFbqk1d4gbxq7Abz0bVjDGQ-twTt0CQcgjYqtLynxqVPKhSZ/pubhtml", # subset-1000 records
    "features_dataset":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"
}


production_data = {
    "stores_dataset" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales_dataset": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQS1tAlWgq16nxeyQ6tIsyfifyWc5u_2kxV5L7Z4Z6hnueKhimkc0OkbF6Ug9_1mgS48-jTiH8-wz1A/pubhtml",
    "features_dataset":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"   
}