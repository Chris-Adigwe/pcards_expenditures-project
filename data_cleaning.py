import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')


def clean_dataframe(df):
    
        
    columns = {
        0:"Division",
        1: 'batch_transaction_id',
        2:'transaction_date',
        3:'card_posting_date',
        4:'merchant_name',
        5:'transaction_amount',
        6:'transaction_currency',
        7:'original_amount',
        8:'original_currency',
        9:'g_l_account',
        10:'g_l_account_description',
        11:'cost_centre_wbs_element_order',
        12:'cost_centre_wbs_element_order_description',
        13:'merchant_type_(mcc)',
        14:'merchant_type_description',
        15:'purpose'     
    }
    
    #drop unwanted columns
    my_list = list(df.columns)
    my_list = my_list[16:]
    
    
    
    df = df.drop(my_list, 1)
    print('unwanted columns dropped')
    
    #rename columns
    df = df.rename(columns=columns)
    print('columns renamed')
#     df['transaction_date'] = df['transaction_date'].fillna('NaT')
#     df['card_posting_date'] = df['card_posting_date'].fillna('NaT')
#     df['transaction_amount'] = df['transaction_amount'].fillna(0)
#     df['original_amount'] = df['original_amount'].fillna(0)
#     df = df.fillna('unknown')

    #drop empty rows with no values
    df = df.drop(df.index[df['transaction_amount']=='Transaction Amt.'])
#     df = df.drop(df.index[df['transaction_date']=='NaT'])
#     df = df.drop(df.index[df['card_posting_date']=='NaT'])
    df = df.dropna()
    print('all null values filled')
    
    #change datatypes
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['card_posting_date'] = pd.to_datetime(df['card_posting_date'])
    df['transaction_amount'] = pd.to_numeric(df['transaction_amount'])
    df['original_amount'] = pd.to_numeric(df['original_amount'])

    #create a year, month, week column
    df['year'] = df['transaction_date'].dt.isocalendar().year
    df['month'] = pd.to_datetime(df['transaction_date']).dt.month
    df['week'] = df['transaction_date'].dt.isocalendar().week
    
    return df     