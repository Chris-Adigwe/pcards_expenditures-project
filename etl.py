#libraries
import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile, re
import shutil
import glob, csv
from datetime import datetime, date
from io import BytesIO
from utility import get_database_conn, delete_files, last_updated, create_table, engine,update_tables,create_table2
from createTable import load_account_dim,load_cost_centre_dim, load_merchant_dim, load_division_dim, load_currency_dim, load_fact,main_table
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
                      'merchant_type_mcc':str,
                      'merchant_type_description':str,
                      'purpose':str}
    
    files = glob.glob('data/raw' + '/*.xlsx')
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    datafiles = [*map(lambda filename: pd.read_excel(filename, skiprows=1,header=None, names=[*columns.keys()], dtype=columns),files)]
    all_data = pd.concat(datafiles, axis=0,ignore_index=True)
    #last_update = last_updated()
    delete_files('data/raw')

    # if last_update == None:
    all_data.replace(to_replace=r'[^a-zA-Z0-9]', value='', regex=True, inplace=True)
    all_data['transaction_date'] = pd.to_datetime(all_data['transaction_date']).dt.strftime('%Y-%m-%d')
    all_data['card_posting_date'] = pd.to_datetime(all_data['card_posting_date']).dt.strftime('%Y-%m-%d')
    all_data.drop_duplicates(inplace=True)
    all_data.drop_duplicates(subset='batch_transaction_id',inplace=True)
    all_data.dropna(subset=['division'], inplace=True)
    all_data.dropna(subset=['batch_transaction_id'], inplace=True)
    all_data.dropna(subset=['transaction_date'], inplace=True)
    all_data['date_downloaded'] = datetime.now().date()
    #all_data.replace('null', np.nan, inplace=True)


    # MERCHANT DIM
    merchant_dim = all_data[['merchant_name','merchant_type_mcc', 'merchant_type_description']].drop_duplicates()
    merchant_dim.to_csv(f'data/merchant/merchant_{file_name}.csv', sep=',', index=False)
    print('merchant dim done')

    # division DIM
    division_dim = all_data[['division']].drop_duplicates()
    division_dim.to_csv(f'data/division/division_{file_name}.csv', sep=',', index=False)  
    print('division dim done')
    


    ## Create the AccountDimension table
    account_dim = all_data[['g_l_account', 'g_l_account_description']].drop_duplicates()
    account_dim.to_csv(f'data/account/account_{file_name}.csv', sep=',', index=False)  
    print('account dim done')



    # Create the CostCentreDimension table
    cost_centre_dim = all_data[['cost_centre_wbs_element_order', 'cost_centre_wbs_element_order_description','original_currency','original_amount','purpose']].drop_duplicates()
    cost_centre_dim.to_csv(f'data/cost_centre/cost_centre_{file_name}.csv', sep=',', index=False)  
    print('cost centre dim done')

   # Create the trans_currency_dim table
    trans_currency_dim = all_data[['transaction_currency']].drop_duplicates()
    trans_currency_dim.to_csv(f'data/trans_currency_dim/trans_currency_{file_name}.csv', sep=',', index=False)  
    print('trans_currency_dim dim done')



    # Update the Fact Table with foreign keys
    all_data = all_data[['division', 'batch_transaction_id','transaction_date','card_posting_date','transaction_amount','transaction_currency','original_amount','original_currency','g_l_account','g_l_account_description','cost_centre_wbs_element_order','cost_centre_wbs_element_order_description','merchant_name','merchant_type_mcc','merchant_type_description','purpose', 'date_downloaded']]
    all_data.to_csv(f'data/fact/fact_{file_name}.csv', sep=',', index=False)
    print("all data moved to csv")


def incremental_load():
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
                      'merchant_type_mcc':str,
                      'merchant_type_description':str,
                      'purpose':str}
    
    files = glob.glob('data/raw' + '/*.xlsx')
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    datafiles = [*map(lambda filename: pd.read_excel(filename, skiprows=1,header=None, names=[*columns.keys()], dtype=columns),files)]
    all_data = pd.concat(datafiles, axis=0,ignore_index=True)
    #last_update = last_updated()
    delete_files('data/raw')

    all_data.replace(to_replace=r'[^a-zA-Z0-9]', value='', regex=True, inplace=True)
    all_data['transaction_date'] = pd.to_datetime(all_data['transaction_date']).dt.strftime('%Y-%m-%d')
    all_data = all_data[pd.to_datetime(all_data['transaction_date']).dt.date > last_updated()]
    all_data['card_posting_date'] = pd.to_datetime(all_data['card_posting_date']).dt.strftime('%Y-%m-%d')
    all_data.drop_duplicates(inplace=True)
    all_data.drop_duplicates(subset='batch_transaction_id',inplace=True)
    all_data.dropna(subset=['division'], inplace=True)
    all_data.dropna(subset=['batch_transaction_id'], inplace=True)
    all_data.dropna(subset=['transaction_date'], inplace=True)
    all_data['date_downloaded'] = datetime.now().date()
    #all_data.replace('null', np.nan, inplace=True)


    # MERCHANT DIM
    merchant_dim = all_data[['merchant_name','merchant_type_mcc', 'merchant_type_description']].drop_duplicates()
    merchant_dim.to_csv(f'data/merchant/merchant_{file_name}.csv', sep=',', index=False)
    print('merchant dim done')

    # division DIM
    division_dim = all_data[['division']].drop_duplicates()
    division_dim.to_csv(f'data/division/division_{file_name}.csv', sep=',', index=False)  
    print('division dim done')
    


    ## Create the AccountDimension table
    account_dim = all_data[['g_l_account', 'g_l_account_description']].drop_duplicates()
    account_dim.to_csv(f'data/account/account_{file_name}.csv', sep=',', index=False)  
    print('account dim done')



    # Create the CostCentreDimension table
    cost_centre_dim = all_data[['cost_centre_wbs_element_order', 'cost_centre_wbs_element_order_description','original_currency','original_amount','purpose']].drop_duplicates()
    cost_centre_dim.to_csv(f'data/cost_centre/cost_centre_{file_name}.csv', sep=',', index=False)  
    print('cost centre dim done')

   # Create the trans_currency_dim table
    trans_currency_dim = all_data[['transaction_currency']].drop_duplicates()
    trans_currency_dim.to_csv(f'data/trans_currency_dim/trans_currency_{file_name}.csv', sep=',', index=False)  
    print('trans_currency_dim dim done')



    # Update the Fact Table with foreign keys
    all_data = all_data[['division', 'batch_transaction_id','transaction_date','card_posting_date','transaction_amount','transaction_currency','original_amount','original_currency','g_l_account','g_l_account_description','cost_centre_wbs_element_order','cost_centre_wbs_element_order_description','merchant_name','merchant_type_mcc','merchant_type_description','purpose', 'date_downloaded']]
    all_data.to_csv(f'data/fact/fact_{file_name}.csv', sep=',', index=False)
    print("all data moved to csv")










def load_data():
     create_table2()
     load_account_dim()
     load_cost_centre_dim()
     load_merchant_dim()
     load_division_dim()
     load_currency_dim()
     load_fact()
     main_table()




