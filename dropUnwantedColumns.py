import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def drop_columns(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS transaction_table')

    # drop columns from general_table
    sql = '''ALTER TABLE copy_general_table 
                DROP "batch_transaction_id",
                DROP "transaction_date",
                DROP "card_posting_date",
                DROP "original_amount",
                DROP "original_currency",
                DROP "g_l_account",
                DROP "g_l_account_description",
                DROP "cost_centre_wbs_element_order",
                DROP "cost_centre_wbs_element_order_description",
                DROP "merchant_type",
                DROP "merchant_type_description",
                DROP "purpose"
                                            '''

    cursor.execute(sql)
    print("columns droped successfully........")



    cursor.execute('grant select on table general_table to public')
    cursor.execute('grant select on table division_table to public')    
    cursor.execute('grant select on table merchant_table to public')    
    cursor.execute('grant select on table transaction_table to public')    
    cursor.execute('grant select on table division_transaction_table to public')  

    conn.commit()

    cursor.close()
    print("Operation columns drop successfully updated")


