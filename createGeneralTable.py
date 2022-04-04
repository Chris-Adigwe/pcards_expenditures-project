import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def general_table(df, dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS general_table')

    # create general table if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS general_table(
                    division text,
                    batch_transaction_id text,
                    transaction_date date,
                    card_posting_date date,
                    merchant_name text,
                    transaction_amount float,
                    transaction_currency text,
                    original_amount float,
                    original_currency text,
                    g_l_account text,
                    g_l_account_description text,
                    cost_centre_wbs_element_order text,
                    cost_centre_wbs_element_order_description text,
                    merchant_type text,
                    merchant_type_description text,
                    purpose text,
                    year integer,
                    month integer,
                    week integer
                    
                    
                    )'''

    cursor.execute(sql)
    print("general_table created successfully........")

    conn.commit()

    cursor.close()
    print("tables created")


