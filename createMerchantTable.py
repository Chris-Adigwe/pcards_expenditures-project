import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def merchant_table(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS merchant_table')

    # create division table if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS merchant_table(
                    merchant_id SERIAL UNIQUE,
                    merchant_name text,
                    merchant_type text,
                    merchant_type_description text
                    )
                    '''

    cursor.execute(sql)
    print("merchant_table created successfully........")

    # insert records
    sql1 = '''insert into merchant_table(merchant_name, merchant_type,                merchant_type_description)
    select distinct merchant_name,merchant_type,merchant_type_description 
    from general_table'''

    cursor.execute(sql1)
    print("merchant_table records inserted successfully........")

    # update general_table
    sql2 = '''update general_table
              set merchant_name =(
                select merchant_id from merchant_table 
                where merchant_table.merchant_name = general_table.merchant_name
                and general_table.merchant_type = merchant_table.merchant_type
                and general_table.merchant_type_description = merchant_table.merchant_type_description
                )'''

    cursor.execute(sql2)
    print("general_table updated successfully........")

    # add merchant_id to general_table
    sql3 = '''alter table general_table
                rename column merchant_name to merchant_id'''

    cursor.execute(sql3)
    print("general_table updated successfully........")

    conn.commit()

    cursor.close()
    print("merchant_table and general_table successfully updated")


