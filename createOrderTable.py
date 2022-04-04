import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
from glob import glob
from io import BytesIO
print('downloading started')

def order_table(dbname, password):
    # establishing the connection
    conn = psycopg2.connect(
        database=f"{dbname}", user='postgres', password=f'{password}', host='127.0.0.1', port='5432')

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS order_table')

    # create transaction table if it does not exist
    sql = '''CREATE TABLE IF NOT EXISTS order_table(
                                            order_id SERIAL UNIQUE,
                                            g_l_account text,
                                            g_l_account_description text,
                                            cost_centre_wbs_element_order text,
                                            cost_centre_wbs_element_order_description text,
											purpose text
                                            
                )
                                            '''

    cursor.execute(sql)
    print("order_table created successfully........")

    # insert records
    sql1 = '''insert into order_table(g_l_account,g_l_account_description,          cost_centre_wbs_element_order,cost_centre_wbs_element_order_description, purpose)
    select distinct general_table.g_l_account,general_table.g_l_account_description,general_table.cost_centre_wbs_element_order,general_table.cost_centre_wbs_element_order_description, general_table.purpose
                            from general_table'''

    cursor.execute(sql1)
    print("order_table records inserted successfully........")

    # alter data type for transaction amount in transaction_table


    # add transaction_id to general_table
    sql2 = '''ALTER TABLE general_table
                ADD order_id integer;'''

    cursor.execute(sql2)
    print("order_id successfully added general_table........")

    # update general_table
    sql6 = '''update general_table
                set order_id =(
                select order_id from order_table 
                where general_table.g_l_account = order_table.g_l_account
                and general_table.g_l_account_description = order_table.g_l_account_description
                and general_table.cost_centre_wbs_element_order = order_table.cost_centre_wbs_element_order
                and general_table.cost_centre_wbs_element_order_description = order_table.cost_centre_wbs_element_order_description
                and general_table.purpose = order_table.purpose
                ) '''

    cursor.execute(sql6)
    print("general_table updated successfully........")

    conn.commit()

    cursor.close()
    print("order_table and general_table successfully updated")


