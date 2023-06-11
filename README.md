# pcards_expenditures-project
Automating the ETL Process: Extracting Excel Files from PCards to PostgreSQL Database
Requirements
Extract, Transform, and Load (ETL) processes are a crucial part of data engineering and data integration workflows. They involve extracting data from various sources, transforming it into a suitable format, and loading it into a target database for analysis and reporting. In this article, we will explore the automation of the ETL process for extracting Excel files from PCards and loading them into a PostgreSQL database.

Introduction
PCards (Procurement Cards) are payment cards used by organizations to streamline their purchasing process. PCards generate transaction data that is often stored in Excel files. Extracting and consolidating this data into a centralized database can provide valuable insights for financial analysis and reporting. By automating the ETL process, we can save time and effort while ensuring data accuracy and consistency.

The ETL Process
The ETL process can be divided into three main steps: extraction, transformation, and loading.

1. Extraction
The first step is to extract the Excel files from the PCards. We will utilize the Toronto Open Data API to retrieve the dataset containing the PCard expenditures. The Python requests library can be used to interact with the API and download the dataset. Once downloaded, we extract the zip file and store the Excel files in a designated folder.

2. Transformation
The next step is to transform the extracted data into a suitable format for loading into the database. We will use the pandas library to read the Excel files, perform data cleansing, and create separate CSV files for each dimension table (e.g., account_dim, merchant_dim, cost_centre_dim, etc.) and the fact table. Additionally, we will convert date columns to the appropriate format and handle missing values.

3. Loading
The final step is to load the transformed data from the CSV files into a PostgreSQL database. We can use the psycopg2 library to connect to the database and execute SQL queries for creating tables and loading data.

Prerequisites
Before we begin, ensure that you have the following requirements in place:

Python installed on your machine.
PostgreSQL installed and running.
Required Python packages installed (pandas, psycopg2, sqlalchemy, dotenv).
Code Overview
The provided code consists of two main components: utility functions and ETL functions.

Utility Functions
The utility functions in the code handle database connections, file management, table creation, and other common tasks. Let's briefly go through each function:

get_database_conn()
This function establishes a connection to the PostgreSQL database using the provided credentials from the .env file.

delete_files(folder)
This function deletes all files in the specified folder. It is used to clean up temporary files generated during the ETL process.

create_table()
This function creates the necessary tables in the PostgreSQL database if they don't already exist. It creates tables for dimensions (e.g., account_dim, merchant_dim) and a fact table.

engine()
This function creates a SQLAlchemy engine to connect to the PostgreSQL database. It is used for executing SQL queries.

last_updated()
This function retrieves the maximum transaction date from the fact table. It can be used to track the last update timestamp.

update_tables()
This function adds foreign key constraints to the fact table.

create_table2()
This function creates an alternative version of the tables with additional columns to hold denormalized data. It creates tables such as main_table and modifies the structure of the fact table.

ETL Functions
The ETL functions handle the extraction, transformation, and loading of data. Let's briefly go through each function:

file_extraction()
This function downloads the Excel files from the Toronto Open Data CKAN instance and extracts them into the data/raw folder.

transform_data()
This function reads the extracted Excel files, performs data cleaning and transformation operations, and saves the transformed data into CSV files. It extracts data for dimensions (e.g., account_dim, merchant_dim) and the fact table.

load_data()
This function creates the necessary tables and loads the transformed data from the CSV files into the PostgreSQL database.

Step-by-Step Process
Now let's walk through the step-by-step process of automating the ETL process using the provided code:

Step 1: Set up the Environment
Ensure that you have all the prerequisites mentioned earlier installed and configured correctly.
Place the code and utility functions in the desired directory on your machine.
Update the file paths in the utility functions to match your environment.
Step 2: Set up the Database Credentials
Create a .env file in the same directory as the code.
Define the required environment variables in the .env file

Step 3: Execute the ETL Process

Open a terminal or command prompt and navigate to the directory where you have placed the code.
Run the following command to install the required Python packages:
python exe.py