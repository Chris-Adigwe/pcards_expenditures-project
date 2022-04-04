import os
import numpy as np
import pandas as pd
import psycopg2
import requests
import zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')


def division_table(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS division_table')

    # create division table if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS division_table(
                            division_id SERIAL UNIQUE,
                            division text
                            )'''

    cursor.execute(sql)
    print("division_table created successfully........")

    # insert records
    sql1 = '''insert into division_table(division)
              select distinct division from general_table'''

    cursor.execute(sql1)
    print("division_table records inserted successfully........")

    # update general_table
    sql2 = '''update general_table
              set division = (select division_id from division_table
              where general_table.division = division_table.division)'''

    cursor.execute(sql2)
    print("general_table updated successfully........")

    # add division_id to general_table
    sql3 = '''alter table general_table
              rename column division to division_id'''

    cursor.execute(sql3)
    print("general_table updated successfully........")

    conn.commit()

    cursor.close()
    print("division_table and general_table successfully updated")



