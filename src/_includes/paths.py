# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 17:14:05
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-25 14:16:18

"""
Contains shared variables
"""

# higher level directories
root_dir = "/mnt/d/Projects/data_tests_demo/retail_data"
scripts_dir = f"{root_dir}/src"
docs_dir = f"{root_dir}/docs"

# data dirs
data_dir = f"{root_dir}/data"
source_data_dir = f"{data_dir}/0_source"
raw_data_dir = f"{data_dir}/1_raw"
transformed_data_dir = f"{data_dir}/2_transformed"

# profile dirs
data_profile_dir = f"{docs_dir}/data_profiles"
source_data_profile_dir = f"{data_profile_dir}/0_source"


# great_expectations
great_expectations_root = f"{docs_dir}/great_expectations"
expectations_suite_dir = f"{great_expectations_root}/expectations"


test_data_urls = {
    "stores" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTCEpWPXGMM_kEg_mElJbUQk_7yFfHeEFbqk1d4gbxq7Abz0bVjDGQ-twTt0CQcgjYqtLynxqVPKhSZ/pubhtml", # subset-1000 records
    "features":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"
}


data_urls = {
    "stores" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
    "sales": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQS1tAlWgq16nxeyQ6tIsyfifyWc5u_2kxV5L7Z4Z6hnueKhimkc0OkbF6Ug9_1mgS48-jTiH8-wz1A/pubhtml",
    "features":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"   
}