# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 09:50:10
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-20 11:10:59

"""
Extracts retail data from websites
And writes them into csv files
"""

from typing import IO

import pandas as pd


def get_data_table(url: str) -> pd.DataFrame:
    """Parses table out from a website

    Args:
        url (str): Website URL

    Returns:
        pd.DataFrame: A pandas dataframe containing the data

    Raises:
        Exception: When the URL is not valid
    """
    try:
        table = pd.read_html(url, header=1,)[
            0
        ].iloc[:, 1:]
    except:
        raise Exception("URL is invalid")
    return table


def write_csv(table: pd.DataFrame, file_name: str) -> IO:
    """Writes table to a csv file

    Args:
        table (pd.DataFrame): Table or data to be written
        file_name (str): File name of csv file

    Returns:
        IO: [description]: A csv file
    """
    return table.to_csv(file_name + ".csv", index=False)


# urls = {
#     "store_dataset" : "https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml",
#     "sales_dataset": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQS1tAlWgq16nxeyQ6tIsyfifyWc5u_2kxV5L7Z4Z6hnueKhimkc0OkbF6Ug9_1mgS48-jTiH8-wz1A/pubhtml",
#     "features_dataset":"https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml"
# }


# print(
#     list(get_data_table(urls["store_dataset"]).columns),
# )

# write_csv(
#     get_table(urls["store_dataset"]).head(),
#     "test"
# )
