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
from datetime import datetime, date

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

CREATE TABLE IF NOT EXISTS account_dim
(   account_id INT,
    g_l_account text,
    g_l_account_description text,
    PRIMARY KEY (account_id)
);


CREATE TABLE IF NOT EXISTS merchant_dim
(   merchant_id INT, 
    merchant_name TEXT, 
    merchant_type_mcc TEXT, 
    merchant_type_description TEXT,
    PRIMARY KEY (merchant_id)
);

CREATE TABLE IF NOT EXISTS cost_centre_dim
(  
    cost_centre_id  INT,
    cost_centre_wbs_element_order TEXT,
    cost_centre_wbs_element_order_description TEXT,
    original_currency TEXT,
    original_amount    float,
    purpose TEXT,
    PRIMARY KEY (cost_centre_id)
);

CREATE TABLE IF NOT EXISTS division_dim
(   
    division_id INT, 
    division TEXT, 
    PRIMARY KEY (division_id)
);

CREATE TABLE IF NOT EXISTS currency_dim
(   
    trans_currency_id INT, 
    transaction_currency TEXT, 
    PRIMARY KEY (trans_currency_id)
);

CREATE TABLE IF NOT EXISTS fact
(   
    batch_transaction_id TEXT NOT NULL,
    transaction_date DATE,
    card_posting_date DATE,
    transaction_amount float,
    date_downloaded date,
    division_id INT NOT NULL,
    account_id	INT NOT NULL,
    merchant_id INT NOT NULL,
    cost_centre_id INT NOT NULL,
    trans_currency_id INT NOT NULL,
   
    
    
   

    PRIMARY KEY (batch_transaction_id))
        ''')
    

    con.commit()
    cur.close()
    con.close()

    print('tables created')

def engine():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME3')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@localhost/{db_name}')
    return engine


def last_updated():
    conn = engine()
    query1 = ''' SELECT MAX(transaction_date) FROM fact'''
    # Execute the query and fetch the results
    with conn.connect() as con:
        df = pd.DataFrame(con.execute(query1)).values.tolist()[0][0]
        # df.to_csv(f'data/questions/question1.csv', index=False)
        
    # last_updated = pd.read_sql(''' SELECT MAX(transaction_date) FROM canada.fact''', con=conn).values.tolist()[0][0]
    # return last_updated

    return df

def update_tables():
    conn = get_database_conn()
    cur = conn.cursor()

    cur.execute('''
    
    ALTER TABLE fact
    ADD CONSTRAINT fk_fact_account_id FOREIGN KEY (account_id) REFERENCES account_dim(account_id) ON DELETE SET NULL ON UPDATE CASCADE,
    ADD CONSTRAINT fk_fact_merchant_id FOREIGN KEY (merchant_id) REFERENCES merchant_dim(merchant_id) ON DELETE SET NULL ON UPDATE CASCADE,
    ADD CONSTRAINT fk_fact_cost_centre_id FOREIGN KEY (cost_centre_id) REFERENCES cost_centre_dim(cost_centre_id) ON DELETE SET NULL ON UPDATE CASCADE,
    ADD CONSTRAINT fk_fact_trans_currency_id FOREIGN KEY (trans_currency_id) REFERENCES currency_dim(trans_currency_id) ON DELETE SET NULL ON UPDATE CASCADE;
    
    
    
    
    
    
    
    
    
    
    ''')


def create_table2():
    con = get_database_conn()
    cur = con.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS account_dim (
            account_id SERIAL,
            g_l_account TEXT,
            g_l_account_description TEXT,
            PRIMARY KEY (account_id)
        );

        CREATE TABLE IF NOT EXISTS merchant_dim (
            merchant_id SERIAL, 
            merchant_name TEXT, 
            merchant_type_mcc TEXT, 
            merchant_type_description TEXT,
            PRIMARY KEY (merchant_id)
        );

        CREATE TABLE IF NOT EXISTS cost_centre_dim (
            cost_centre_id SERIAL,
            cost_centre_wbs_element_order TEXT,
            cost_centre_wbs_element_order_description TEXT,
            original_currency TEXT,
            original_amount FLOAT,
            purpose TEXT,
            PRIMARY KEY (cost_centre_id)
        );

        CREATE TABLE IF NOT EXISTS division_dim (
            division_id SERIAL, 
            division TEXT, 
            PRIMARY KEY (division_id)
        );

        CREATE TABLE IF NOT EXISTS currency_dim (
            trans_currency_id SERIAL, 
            transaction_currency TEXT, 
            PRIMARY KEY (trans_currency_id)
        );

        CREATE TABLE IF NOT EXISTS fact (
            division TEXT,   
            batch_transaction_id TEXT,
            transaction_date DATE,
            card_posting_date DATE,
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
            date_downloaded DATE,
            PRIMARY KEY (batch_transaction_id)
        );

        CREATE TABLE IF NOT EXISTS main_table (
            transaction_id SERIAL,
            batch_transaction_id TEXT,
            transaction_date DATE,
            card_posting_date DATE,
            transaction_amount FLOAT,
            date_downloaded DATE,
            division_id INT,
            account_id INT,
            merchant_id INT,
            cost_centre_id INT,
            trans_currency_id INT,
            PRIMARY KEY (transaction_id),
            CONSTRAINT fk_fact_account_id FOREIGN KEY (account_id) REFERENCES account_dim(account_id) ON DELETE SET NULL ON UPDATE CASCADE,
            CONSTRAINT fk_fact_fact_id FOREIGN KEY (batch_transaction_id) REFERENCES fact(batch_transaction_id) ON DELETE SET NULL ON UPDATE CASCADE,
            CONSTRAINT fk_fact_merchant_id FOREIGN KEY (merchant_id) REFERENCES merchant_dim(merchant_id) ON DELETE SET NULL ON UPDATE CASCADE,
            CONSTRAINT fk_fact_cost_centre_id FOREIGN KEY (cost_centre_id) REFERENCES cost_centre_dim(cost_centre_id) ON DELETE SET NULL ON UPDATE CASCADE,
            CONSTRAINT fk_fact_trans_currency_id FOREIGN KEY (trans_currency_id) REFERENCES currency_dim(trans_currency_id) ON DELETE SET NULL ON UPDATE CASCADE
        );
    ''')

    con.commit()
    cur.close()
    con.close()

    print('Tables created')


