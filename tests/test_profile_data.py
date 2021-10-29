# -*- coding: utf-8 -*-
# @Author: Marylette B. Roa
# @Date:   2021-10-20 10:20:59
# @Last Modified by:   Marylette B. Roa
# @Last Modified time: 2021-10-29 17:00:14

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import datatest as dt
import pytest
from os.path import exists
from glob import glob

from src._includes.paths import raw_data_profile_dir


