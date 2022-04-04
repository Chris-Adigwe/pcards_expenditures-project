import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def transaction_table(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS transaction_table')

    # create transaction table if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS transaction_table(
                                            transaction_id SERIAL UNIQUE,
                                            batch_transaction_id text,
                                            year INTEGER,
                                            month INTEGER,
                                            week INTEGER,
                                            transaction_amount text
                                            )
                                            '''

    cursor.execute(sql)
    print("transaction_table created successfully........")

    #alter general_table transaction amount datatype
    sql3 = '''ALTER TABLE general_table 
              ALTER COLUMN transaction_amount TYPE INT
              USING transaction_amount::integer
            '''

    cursor.execute(sql3)
    print("transaction_amount datatype successfully changed........")

    # insert records
    sql1 = '''insert into transaction_table(batch_transaction_id,year,month,week, transaction_amount)
    select distinct general_table.batch_transaction_id,general_table.year,general_table.month,general_table.week, general_table.transaction_amount
    from general_table'''

    cursor.execute(sql1)
    print("transaction_table records inserted successfully........")

    # alter data type for transaction amount in transaction_table
    sql4= '''ALTER TABLE transaction_table 
                ALTER COLUMN transaction_amount TYPE INT
                USING transaction_amount::integer'''

    cursor.execute(sql4)
    print("transaction_amount datatype successfully changed for  transaction_table........")

    # add transaction_id to general_table
    sql2 = '''ALTER TABLE general_table
                ADD transaction_id integer'''

    cursor.execute(sql2)
    print("transaction_id successfully added general_table........")

    # update general_table
    sql6 = '''update general_table
                    set transaction_id =(
                    select transaction_id from transaction_table 
                    where general_table.batch_transaction_id = transaction_table.batch_transaction_id
                    and general_table.year = transaction_table.year
                    and general_table.month = transaction_table.month
                    and general_table.week = transaction_table.week
                    and general_table.transaction_amount = transaction_table.transaction_amount	
                            )  '''

    cursor.execute(sql6)
    print("general_table updated successfully........")

    conn.commit()

    cursor.close()
    print("transaction_table and general_table successfully updated")


