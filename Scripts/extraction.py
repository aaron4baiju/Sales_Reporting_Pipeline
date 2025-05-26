import pandas as pd
from db_connect import get_db_engine
import logging

#Extract from MySQL
try:
    def extract_from_csv(file_path):
        df_web_sales=pd.read_csv(file_path)
        logging.info("Data Extracted from CSV File to DataFrame.")
        print('Extracted data from CSV File')
        return df_web_sales
except Exception as e:
    logging.error("Error Extracting data fromm CSV",e)

# Extract existing data from MySQL
def extract_from_mysql(table_name):
    engine = get_db_engine()
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=engine)
        logging.info(f"Extracted {len(df)} rows from MySQL table `{table_name}`")
        print('Extracted historical data from MySQL table')
        return df
    except Exception as e:
        logging.warning(f"No existing data found in `{table_name}` or failed to connect: {e}")
        return pd.DataFrame()
