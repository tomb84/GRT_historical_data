#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 15:47:20 2023

@author: tom
"""

import glob
import os
import os.path
import time
import re
import sys
import datetime as dt

from timeit import default_timer as timer
import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
from openpyxl import load_workbook
import boto3
import openpyxl


from configparser import ConfigParser
from sqlalchemy import create_engine # New Redshift upload
import psycopg2 as db # Access Redshift

###GRT Historical data upload

#Three Tasks
#  -1 Read in csv file
#  -2 Clean file to match table schema
#  -3 Upload data to Redshift
#Check that Year is always the same

Year = 2022


os.chdir("/Users/tom/Desktop/GRT Historical")
os.getcwd()

df = pd.read_excel('ResultsGRT 2022 client_pulse.xlsx')

