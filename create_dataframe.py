import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def create_dataframe(file_path):
        #columns
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
    
    
    
    
    
    
        import glob
        files = glob.glob(file_path + '/*.xlsx')
        li = []
    
        for f in files:
            # read in csv
            temp_df = pd.read_excel(f, index_col=None, header=None)
            # append df to list
            li.append(temp_df)
    
    
        df = pd.concat(li)
        print("dataframe created")
    
        return df









