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


def create_database():
    # enter databse name
    dbname = input("Enter your database: ")

    # enter password
    password = input("Enter your password: ")
    print(password)

    # establishing the connection
    conn = psycopg2.connect(
        database="postgres", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    conn.autocommit = True

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    sql1 = f'''DROP DATABASE IF EXISTS {dbname}'''
    cursor.execute(sql1)

    # Preparing query to create a database
    sql = f'''CREATE database {dbname}'''

    # Creating a database
    cursor.execute(sql)
    print("Database created successfully........")

    # Closing the connection
    conn.close()

    return dbname, password
