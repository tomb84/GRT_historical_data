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
#from openpyxl import load_workbook
import boto3
import openpyxl


from configparser import ConfigParser
from sqlalchemy import create_engine # New Redshift upload
import psycopg2 as db # Access Redshift

# Get Redshift information
def get_redshift_creds():
    config = ConfigParser()
    config.read([os.path.join(os.path.expanduser("~"),'.aws/redshift.txt')])
    info = [config.get("default", 'usr'),
            config.get("default", 'pwd'),
            config.get("default", 'host'),
            config.get("default", 'port'),
            config.get("default", 'dbname')]
    return(info)



# Open connections, cursors
def access_redshift():
    redshift_info = get_redshift_creds()
    con=db.connect(user=redshift_info[0], password=redshift_info[1],
                   host=redshift_info[2], port=redshift_info[3],
                   dbname = redshift_info[4])
    cur=con.cursor()
    return(con, cur)

#We have read permsision for prod
def get_redshift_creds_prod():
    redshift_info = get_redshift_creds()
    redshift_creds_prod = {'usr': redshift_info[0],
     'pwd': redshift_info[1],
     'host': redshift_info[2],
     'port': redshift_info[3],
     'dbname': 'prod'}
    
    return redshift_creds_prod

#We have write access to dev
def get_redshift_creds_dev():
    redshift_info = get_redshift_creds()
    redshift_creds_dev = {'usr': redshift_info[0],
     'pwd': redshift_info[1],
     'host': redshift_info[2],
     'port': redshift_info[3],
     'dbname': 'dev'}
    
    return redshift_creds_dev

def redshift_create_engine_noschema_s3_dev(redshift_creds_dev):
    
    sp_url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(redshift_creds_dev['usr'],
                                                       redshift_creds_dev['pwd'],
                                                       redshift_creds_dev['host'],
                                                       redshift_creds_dev['port'],
                                                       redshift_creds_dev['dbname'])

    sp_engine = create_engine(sp_url, echo=False)

    return sp_engine

def redshift_create_engine_noschema_s3_prod(redshift_creds_prod):
    
    sp_url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(redshift_creds_prod['usr'],
                                                       redshift_creds_prod['pwd'],
                                                       redshift_creds_prod['host'],
                                                       redshift_creds_prod['port'],
                                                       redshift_creds_prod['dbname'])

    sp_engine = create_engine(sp_url, echo=False)

    return sp_engine

def get_query_s3(engine, query):
    # create connection
    con = engine.connect()
    # pull data and convert to dataframe
    df = pd.read_sql_query(query, con)
    con.close()
    return df

###GRT Historical data upload

#Three Tasks
#  -1 Read in csv file
#  -2 Clean file to match table schema
#  -3 Upload data to Redshift
#Check that Year is always the same


os.chdir("/Users/tom/Documents/Code/GRT_historical_data")
os.getcwd()


def forward_fill_nans(_df):
    
   #forward fill nans
   _df.fillna(method='ffill', inplace=True) 
   
   return _df

def clean_demo_id_column(_df):
    
    #Clean demo_id col
    _df['demo_type_id'] = _df['demo_type_id'].str.replace('demo_id_', '')
    _df['demo_type_id'] = pd.to_numeric(_df['demo_type_id'])
    
    return _df 

def set_std_col_datatypes_to_integer(_df):
    
    #set dtypes to integer
    col_set_int = ['year','month','country_id','company_id','rating_id',
                  'industry_id','stakeholder_id','demo_type_id','demo_id']
    
    for v in col_set_int:
        _df[v] = _df[v].apply(np.int64)
    
    return _df

def add_missing_cols(_df, _cols_to_add):

    for c in _cols_to_add:
        _df[c]=99999
    
    return _df


def init_col_names(_extra_cols):
    
    std_cols = ['year','month','country_id','company_id','rating_id',
                  'industry_id','stakeholder_id','demo_type_id','demo_id',
                  'batname']

    _table_cols = std_cols + _extra_cols
    
    return _table_cols

#To Delete existing data
def get_delete_query_4_scores( schema, table_name, year, month):
    query = """DELETE from {0}.{1}
            WHERE year = {2}
            AND month = {3}""".format(schema, table_name, year, month)
    return query

def execute_sql(engine, query):
    # create connection
    con = engine.connect()
    # Execute query (delete command)
    con.execute(query)
    con.close()



### MODULES DEPENDENT ON DATA_FILE NAME

def open_data_file(data_file):
    
    rows_to_skip = 1
    
    files_to_skip_2_rows=['dimensionreach']
    
    if any([x in data_file for x in files_to_skip_2_rows]):
        rows_to_skip=2
    
    _df = pd.read_excel(data_file, skiprows=rows_to_skip)   

    return _df

def append_r_G_reb_to_end_of_attribute_id(data_file,_df):
        
    files_to_add_r_G_reb=['attributereach']
    
    if any([x in data_file for x in files_to_add_r_G_reb]):    
        _df['attribute_id'] = _df['attribute_id']+'r_G_reb'
    
    return _df

def multiply_reach_by_100(data_file,_df):
        
    files_to_multiply_reach=['attributereach']
    
    if any([x in data_file for x in files_to_multiply_reach]): 
        print('Multiplying reach by 100')
        _df['b2_percent']=_df['b2_percent']*100.
        _df['m3_percent']=_df['m3_percent']*100.
        _df['t2_percent']=_df['t2_percent']*100.
        _df['not_sure_percent']=_df['not_sure_percent']*100.
        
    return _df

def get_missing_non_float_cols(_data_file):
    
    #Here we need to add all the non-float additional colums
    #Float cols will be set to nans
    _cols_to_add=[]
    if "client_pulse" in data_file:
        _cols_to_add = ['number_of_samples_pulse_score','number_of_samples_smooth_pulse_score']
    
    if "attributeagg" in data_file:
        _cols_to_add = ['number_of_samples']
        
    if "attributereach" in data_file:
        _cols_to_add = ['number_of_samples']    
        
    return _cols_to_add

def add_dimension_id_col(data_file,_df):
    
    _map_cols=[]    
    
    files_to_add_dimension_id = ["attributeagg", "attributereach"]

    if any([x in data_file for x in files_to_add_dimension_id]):
        
        query = """select attribute_id, dimension_id from crt_dim.attribute_dimension
                """
        dim_att_map =  get_query_s3(engine_dev, query) 
        
        MERGE_ON_COL=['attribute_id']
        _df=_df.merge(dim_att_map,how='left', on=MERGE_ON_COL) 
        
        _map_cols = list(dim_att_map.columns)
        _map_cols.remove(MERGE_ON_COL[0])
    
    return _df, _map_cols


def get_data_file_specific_cols(_data_file):
    
    if "client_pulse" in data_file:
       _file_specific_cols = ['Metric','Mean','Count']
       
    if "attributeagg" in data_file:
          _file_specific_cols = ['Metric','Mean','Count']
          
    if "attributereach" in data_file:
          _file_specific_cols = ['Unnamed: 10','B2','M3','T2','99']      
       
    return _file_specific_cols   

def update_existing_col_names(_df, _data_file):
    
    if "client_pulse" in data_file:
        _df.rename(columns = {'Mean':'client_pulse'}, inplace = True)
        _df.rename(columns = {'Count':'resp_count'}, inplace = True)
        
        _file_specific_cols_renamed = ['client_pulse','resp_count']
        
    if "attributeagg" in data_file:
        _df.rename(columns = {'Metric':'attribute_id'}, inplace = True)
        _df.rename(columns = {'Mean':'attribute_pulse'}, inplace = True)
        _df.rename(columns = {'Count':'resp_count'}, inplace = True)
        
        _file_specific_cols_renamed = ['attribute_id','attribute_pulse','resp_count']
        
    if "attributereach" in data_file:
        _df.rename(columns = {'Unnamed: 10':'attribute_id'}, inplace = True)
        _df.rename(columns = {'B2':'b2_percent'}, inplace = True)
        _df.rename(columns = {'M3':'m3_percent'}, inplace = True)
        _df.rename(columns = {'T2':'t2_percent'}, inplace = True)
        _df.rename(columns = {'99':'not_sure_percent'}, inplace = True)
        
        _file_specific_cols_renamed = ['attribute_id','b2_percent','m3_percent','t2_percent','not_sure_percent']    
        
    return _df, _file_specific_cols_renamed

###########################################################

data_file = 'Data/ResultsGRT 2020 attributereach.xlsx'


df=pd.DataFrame()
df=open_data_file(data_file)
df=forward_fill_nans(df)


file_specific_cols = get_data_file_specific_cols(data_file)
table_cols = init_col_names(file_specific_cols)

#set the col names
df.columns = table_cols

######################################################
#WARNING DEMO_TYPE_ID WAS INCORRECTLY SET IN SPSS SCRIPT
## DEMO_ID_2 SHOULD BE DEMO_ID_1
df['demo_type_id'] = df['demo_type_id'].str.replace('demo_id_2', 'demo_id_1')
######################################################

df=clean_demo_id_column(df) 
df['demo_type_id'].value_counts()

df=set_std_col_datatypes_to_integer(df)

#Add extra cols in db that are not in the file
cols_to_add = get_missing_non_float_cols(data_file)
df=add_missing_cols(df, cols_to_add)

#Year should be set to the year before.
df['year']=df['year']-1
df['year'].value_counts()
#Month should always be 12
df['month']=12

df,file_specific_cols_renamed=update_existing_col_names(df, data_file)

df=append_r_G_reb_to_end_of_attribute_id(data_file,df)

df=multiply_reach_by_100(data_file,df)


#Now write to database
redshift_creds_dev=get_redshift_creds_dev()
engine_dev = redshift_create_engine_noschema_s3_dev(redshift_creds_dev)

df,map_cols = add_dimension_id_col(data_file,df)

valid_cols=list(df.columns)
print(valid_cols)

#keep only relevant cols
df_upload  = df[['year','month','country_id','company_id','rating_id',
              'industry_id','stakeholder_id','demo_type_id','demo_id']+
              file_specific_cols_renamed+cols_to_add+map_cols]

#Check that file format looks good
df_upload.iloc[0]
df_upload.dtypes



##PARAMETERS FOR UPLOAD
#How should we set the values here. - ask at meeting tomorrow.
YEAR=df['year'].unique()[0]
MONTH=12
#CHECK THIS WORKS FOR ALL TABLES
TABLE_NAME = data_file.split(' ')[2].split('.')[0]
print(YEAR, MONTH, TABLE_NAME)

#Remove any existing data that matches this upload
query = get_delete_query_4_scores('grt_ds_agg', TABLE_NAME, str(YEAR), str(MONTH))
execute_sql(engine_dev, query)  

# Would fail if your dataframe has a schema different than the destination table
print(df_upload.shape,df_upload['year'].unique())
df_upload.to_sql(name=TABLE_NAME, con=engine_dev, index=False, schema= 'grt_ds_agg', if_exists='append', chunksize=1000, method='multi')



