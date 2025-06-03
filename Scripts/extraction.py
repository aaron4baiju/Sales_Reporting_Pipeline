#Extracts FRESH DATA from CSV files (DATA RESOURCES) and EXISTING DATA from MySQL DB into DataFrames.

import pandas as pd
from db_connect import get_db_engine
import logging

#Extract from MySQL
def extract_from_csv(file_path):
    try:
        df1=pd.read_csv(file_path)
        logging.info(f"Data Extracted from CSV File to DataFrame {file_path}")
        return df1
    except Exception as e:
        logging.error(f"Error Extracting data fromm CSV {e}")

# Extract existing data from MySQL
def extract_from_mysql(table_name):
    engine = get_db_engine()
    try:
        query = f"SELECT * FROM {table_name}"
        df2 = pd.read_sql(query, con=engine)
        logging.info(f"Extracted {len(df2)} rows from MySQL table `{table_name}`")
        return df2
    except Exception as e:
        logging.warning(f"No existing data found in `{table_name}` or failed to connect: {e}")
        return pd.DataFrame()
