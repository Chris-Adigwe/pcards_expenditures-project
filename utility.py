import os
import tempfile
import pandas as pd
import requests
import datetime
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import dotenv, os
from dotenv import dotenv_values
import os
import glob

def get_database_conn():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME3')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    conn = psycopg2.connect(f'dbname={db_name} user={db_user_name} password={db_password} host={host} port={port}')

    return conn



def delete_files(folder):
    files = os.listdir(folder)
    for file in files:
        file_path = os.path.join(folder, file)
        os.remove(file_path)
        print(f"Deleted file: {file}")


def create_table():
    con = get_database_conn()
    cur = con.cursor()


    cur.execute('''  



CREATE TABLE IF NOT EXISTS account_dim (
    ID SERIAL PRIMARY KEY,
    g_l_account TEXT,
    g_l_account_description TEXT,
    CONSTRAINT account_dim UNIQUE (ID)
);

CREATE TABLE IF NOT EXISTS division_dim (
    ID SERIAL PRIMARY KEY,
    division TEXT,
    CONSTRAINT division_dim UNIQUE (ID)
);

CREATE TABLE IF NOT EXISTS merchant_dim (
    ID SERIAL PRIMARY KEY,
    merchant_name TEXT,
    merchant_type_description TEXT,
    CONSTRAINT merchant_dim UNIQUE (ID)
);

CREATE TABLE IF NOT EXISTS cost_centre_dim (
    ID SERIAL PRIMARY KEY,
    cost_centre_wbs_element_order TEXT,
    cost_centre_wbs_element_order_description TEXT,
    CONSTRAINT cost_centre_dim UNIQUE (ID)
);

CREATE TABLE IF NOT EXISTS canada.fact (
    division TEXT,
    batch_transaction_id TEXT PRIMARY KEY,
    transaction_date DATE,
    card_posting_date DATE,
    merchant_name TEXT,
    transaction_amount FLOAT,
    transaction_currency TEXT,
    original_amount FLOAT,
    original_currency TEXT,
    g_l_account TEXT,
    g_l_account_description TEXT,
    cost_centre_wbs_element_order TEXT,
    cost_centre_wbs_element_order_description TEXT,
    merchant_type TEXT,
    merchant_type_description TEXT,
    purpose TEXT,
    CONSTRAINT fact UNIQUE (batch_transaction_id)
)
    						


    
	
        ''')
    

    con.commit()
    cur.close()
    con.close()

    print('tables created')




create_table()





