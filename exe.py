# import the modules
import os
import numpy as np
import pandas as pd
import psycopg2
import requests
import zipfile
import shutil
from glob import glob
from etl import file_extraction, transform_data, load_data, incremental_load
from utility import last_updated


def main():
    file_extraction()

    if last_updated() == None:
        transform_data()
    else:
        incremental_load()

    load_data()
    
main()