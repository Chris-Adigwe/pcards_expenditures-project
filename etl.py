#libraries
import os
import numpy as np
import pandas as pd
import psycopg2
import requests, zipfile
import shutil
import glob
from io import BytesIO
print('downloading started')

#create file url, filepath
def file_extraction(url, file_path):
    
    #getting the data
    req = requests.get(url)
    print('Downloading completed')
    
    #creating file directory
    path = os.path.join(os.getcwd(), file_path)
    
    if not os.path.exists(path):
        os.mkdir(path)
    
    else:
        shutil.rmtree(path)
        os.mkdir(path)
        
    print("directory created")
    
    zip_folder = zipfile.ZipFile(BytesIO(req.content))
    zip_folder.extractall(path)
    print('datasets extracted')
    
    return path



url = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/ebc3f9c2-2f80-4405-bf4f-5fb309581485/resource/d83a5249-fb07-4c38-9145-9e12a32ce1d4/download/pcard-expenses.zip'


