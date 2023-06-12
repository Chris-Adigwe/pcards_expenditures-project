
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
print('downloading started')

def load_account_dim():
    conn = get_database_conn()
    cur = conn.cursor()

    account_dim = glob.glob('data/account/*.csv')[0]


    cur.execute('''
        CREATE TEMPORARY TABLE temp_account(   
                g_l_account text,
                g_l_account_description text)
    
    ''')

    #for the account table
    print('data loading for temp_account table')
    with open(account_dim, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_account', sep=',')

    cur.execute('''
    INSERT INTO account_dim (
        g_l_account,
        g_l_account_description
    )
    SELECT 
        g_l_account,
        g_l_account_description
    FROM temp_account
    EXCEPT
    SELECT
        g_l_account,
        g_l_account_description
    FROM account_dim
        ''')


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/account')

    print('data successfully loaded to account_dim')


def load_cost_centre_dim():
    conn = get_database_conn()
    cur = conn.cursor()

    df = glob.glob('data/cost_centre/*.csv')[0]


    cur.execute('''
        CREATE TEMPORARY TABLE temp_cost_centre(   
                cost_centre_wbs_element_order TEXT,
                cost_centre_wbs_element_order_description TEXT,
                original_currency TEXT,
                original_amount    float,
                purpose TEXT)
    
    ''')

    #for the account table
    print('data loading for cost_centre_dim table')
    with open(df, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_cost_centre', sep=',')

    cur.execute('''
    INSERT INTO cost_centre_dim (
                cost_centre_wbs_element_order,
                cost_centre_wbs_element_order_description,
                original_currency,
                original_amount,
                purpose
    )
    SELECT 
                cost_centre_wbs_element_order,
                cost_centre_wbs_element_order_description,
                original_currency,
                original_amount,
                purpose
    FROM temp_cost_centre 
    EXCEPT
    SELECT
                cost_centre_wbs_element_order,
                cost_centre_wbs_element_order_description,
                original_currency,
                original_amount,
                purpose
    FROM cost_centre_dim
        ''')


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/cost_centre')

    print('data successfully cost_centre_dim table')



def load_merchant_dim():
    conn = get_database_conn()
    cur = conn.cursor()

    df = glob.glob('data/merchant/*.csv')[0]


    cur.execute('''
        CREATE TEMPORARY TABLE temp_merchant_dim(   
                merchant_name TEXT, 
                merchant_type_mcc TEXT, 
                merchant_type_description TEXT
                
                )
    
    ''')

    #for the account table
    print('data loading for merchant_dim table')
    with open(df, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_merchant_dim', sep=',')

    cur.execute('''
    INSERT INTO merchant_dim (
                merchant_name, 
                merchant_type_mcc, 
                merchant_type_description
    )
    SELECT 
                merchant_name, 
                merchant_type_mcc, 
                merchant_type_description
    FROM temp_merchant_dim
    EXCEPT
    SELECT
                merchant_name, 
                merchant_type_mcc, 
                merchant_type_description
    FROM merchant_dim
        ''')


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/merchant')

    print('data successfully merchant_dim table')



def load_division_dim():
    conn = get_database_conn()
    cur = conn.cursor()

    df = glob.glob('data/division/*.csv')[0]


    cur.execute('''
        CREATE TEMPORARY TABLE temp_division_dim(   
                division TEXT
                
                )
    
    ''')

    #for the account table
    print('data loading for division_dim table')
    with open(df, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_division_dim', sep=',')

    cur.execute('''
    INSERT INTO division_dim (
                division
    )
    SELECT 
                division
    FROM temp_division_dim
    EXCEPT
    SELECT
                division
    FROM division_dim
        ''')


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/division')

    print('data successfully division_dim table')


def load_currency_dim():
    conn = get_database_conn()
    cur = conn.cursor()

    df = glob.glob('data/trans_currency_dim/*.csv')[0]


    cur.execute('''
        CREATE TEMPORARY TABLE temp_currency_dim(   
                transaction_currency TEXT
                
                )
    
    ''')

    #for the account table
    print('data loading for currency_dim table')
    with open(df, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_currency_dim', sep=',')

    cur.execute('''
    INSERT INTO currency_dim (
                transaction_currency
    )
    SELECT 
                transaction_currency
    FROM temp_currency_dim
    EXCEPT
    SELECT
                transaction_currency
    FROM currency_dim
        ''')


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/trans_currency_dim')

    print('data successfully currency_dim table')





def load_fact():
    conn = get_database_conn()
    cur = conn.cursor()

    fact = glob.glob('data/fact/*.csv')[0]

    #CREATE A TEMP TABLE FOR FACT
    cur.execute('''
    CREATE TEMPORARY TABLE temp_fact (
        division TEXT,   
        batch_transaction_id TEXT,
        transaction_date TEXT,
        card_posting_date TEXT,
        transaction_amount FLOAT,
        transaction_currency TEXT,
        original_amount FLOAT,
        original_currency TEXT,
        g_l_account TEXT,
        g_l_account_description TEXT,
        cost_centre_wbs_element_order TEXT,
        cost_centre_wbs_element_order_description TEXT,
        merchant_name TEXT,
        merchant_type_mcc TEXT,
        merchant_type_description TEXT,
        purpose TEXT,
        date_downloaded DATE
    )
''')

    


    #for the fact table
    print('data loading for fact table')
    with open(fact, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, 'temp_fact', sep=',')


    cur.execute('''
    INSERT INTO fact (
        division,   
        batch_transaction_id,
        transaction_date,
        card_posting_date,
        transaction_amount,
        transaction_currency,
        original_amount,
        original_currency,
        g_l_account,
        g_l_account_description,
        cost_centre_wbs_element_order,
        cost_centre_wbs_element_order_description,
        merchant_name,
        merchant_type_mcc,
        merchant_type_description,
        purpose,
        date_downloaded
    )
    SELECT 
        division,   
        batch_transaction_id,
        CASE
            WHEN transaction_date = '' THEN NULL
            ELSE TO_DATE(transaction_date, 'YYYY-MM-DD')
        END,
        CASE
            WHEN card_posting_date = '' THEN NULL
            ELSE TO_DATE(card_posting_date, 'YYYY-MM-DD')
        END,
        transaction_amount,
        transaction_currency,
        original_amount,
        original_currency,
        g_l_account,
        g_l_account_description,
        cost_centre_wbs_element_order,
        cost_centre_wbs_element_order_description,
        merchant_name,
        merchant_type_mcc,
        merchant_type_description,
        purpose,
        date_downloaded
    FROM temp_fact
    EXCEPT
    SELECT
        division,   
        batch_transaction_id,
        transaction_date::DATE,
        card_posting_date::DATE,
        transaction_amount,
        transaction_currency,
        original_amount,
        original_currency,
        g_l_account,
        g_l_account_description,
        cost_centre_wbs_element_order,
        cost_centre_wbs_element_order_description,
        merchant_name,
        merchant_type_mcc,
        merchant_type_description,
        purpose,
        date_downloaded
    FROM fact
''')
    


    conn.commit()
    cur.close()
    conn.close()
    delete_files('data/fact')

    print('data successfully FACT loaded to table')



def main_table():
    conn = get_database_conn()
    cur = conn.cursor()


    cur.execute('''
    INSERT INTO main_table(
                batch_transaction_id,
                transaction_date,
                card_posting_date,
                transaction_amount,
                date_downloaded,
                division_id,
                account_id,
                merchant_id,
                cost_centre_id,
                trans_currency_id
    )
    SELECT 
                fact.batch_transaction_id,
                fact.transaction_date,
                fact.card_posting_date,
                fact.transaction_amount,
                fact.date_downloaded,
                division_dim.division_id,
                account_dim.account_id,
                merchant_dim.merchant_id,
                cost_centre_dim.cost_centre_id,
                currency_dim.trans_currency_id
    FROM fact, division_dim, account_dim, merchant_dim, cost_centre_dim, currency_dim

    WHERE fact.division = division_dim.division
    AND   fact.g_l_account = account_dim.g_l_account 
    AND   fact.g_l_account_description  = account_dim.g_l_account_description 
    AND   fact.transaction_currency = currency_dim.transaction_currency
    AND   fact.merchant_name = merchant_dim.merchant_name
    AND   fact.merchant_type_mcc = merchant_dim.merchant_type_mcc
    AND   fact.merchant_type_description =  merchant_dim.merchant_type_description  
    AND   fact.cost_centre_wbs_element_order = cost_centre_dim.cost_centre_wbs_element_order
    AND   fact.cost_centre_wbs_element_order_description = cost_centre_dim.cost_centre_wbs_element_order_description
    AND  fact.original_currency = cost_centre_dim.original_currency
    AND fact.original_amount   = cost_centre_dim.original_amount
    AND  fact.purpose  = cost_centre_dim.purpose

    EXCEPT
    SELECT
                batch_transaction_id,
                transaction_date,
                card_posting_date,
                transaction_amount,
                date_downloaded,
                division_id,
                account_id,
                merchant_id,
                cost_centre_id,
                trans_currency_id
    FROM main_table

   
''')
    
    
    


    conn.commit()
    cur.close()
    conn.close()

    print('data successfully FACT loaded to table')