#libraries
import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
import glob
from datetime import datetime, timedelta
from io import BytesIO
from utility import get_database_conn, delete_files
print('downloading started')

#create file url, filepath
def file_extraction():
    
    # Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
    # https://docs.ckan.org/en/latest/api/
 
    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
 
    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = { "id": "pcard-expenditures"}
    package = requests.get(url, params = params).json()
 
    # To get resource data:
    data  = package.get('result').get('resources')[1].get('url')

    response = requests.get(data)

    zip_folder = zipfile.ZipFile(BytesIO(response.content))
    zip_folder.extractall('data/raw')
    print('datasets extracted')


def transform_data():
    columns =        { 'division':str,
                      'batch_transaction_id':str,
                      'transaction_date':'datetime64[ns]',
                      'card_posting_date':'datetime64[ns]',
                      'merchant_name':str,
                      'transaction_amount':float,
                      'transaction_currency':str,
                      'original_amount':float,
                      'original_currency':str,
                      'g_l_account':str,
                      'g_l_account_description':str,
                      'cost_centre_wbs_element_order':str,
                      'cost_centre_wbs_element_order_description':str,
                      'merchant_type_(mcc)':str,
                      'merchant_type_description':str,
                      'purpose':str}
    
    files = glob.glob('data/raw' + '/*.xlsx')
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    datafiles = [*map(lambda filename: pd.read_excel(filename, skiprows=1,header=None, names=[*columns.keys()], dtype=columns),files)]
    all_data = pd.concat(datafiles, axis=0,ignore_index=True)



    all_data['transaction_date'] = pd.to_datetime(all_data['transaction_date'])
    all_data['card_posting_date'] = pd.to_datetime(all_data['card_posting_date'])
    all_data.drop_duplicates(inplace=True)
    all_data.drop_duplicates(subset='batch_transaction_id',inplace=True)
    all_data.dropna(subset=['batch_transaction_id'], inplace=True)
    all_data['date_downloaded'] = datetime.now()
    all_data.to_csv(f'data/transformed/fact/fact_{file_name}.csv', index=False)
    delete_files('data/raw')


#file_extraction()
transform_data()