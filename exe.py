# import the modules
import glob
from create_dataframe import *
from create_database import *
from createDivisionTable import *
from createGeneralTable import *
from createMerchantTable import *
from createTransactionTable import *
from createOrderTable import *
from data_cleaning import *
from division_transaction_table import *
from dropUnwantedColumns import *
from etl import *

# import the packages
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

# extract the excel files
file_path = file_extraction(url, "datasets")

# create a dataframe from the excel files
import glob
df = create_dataframe(file_path)

# clean dataframe file
df = clean_dataframe(df)

# create a database
dbname, password = create_database()

#create master table
general_table(df, dbname, password)

#create division table
division_table(dbname, password)

#create merchant_table
merchant_table(dbname, password)

#create transaction table
transaction_table(dbname, password)

#create order table
order_table(dbname, password)

#create division_transaction_table
division_transaction_table(dbname, password)

#dropUnwantedColumns
drop_columns(dbname, password)

