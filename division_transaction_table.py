import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def division_transaction_table(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS transaction_table')

    # drop columns from general_table
    sql = '''create table division_transaction_table as(
                    SELECT division_id, year, week, SUM (transaction_amount) AS total
                    FROM general_table
                    group by division_id, year, week
                    order by year, week asc
                        )
                                            '''

    cursor.execute(sql)
    print("division_transaction_table successfully........")
 

    conn.commit()

    cursor.close()
    print("Operation columns drop successfully updated")


