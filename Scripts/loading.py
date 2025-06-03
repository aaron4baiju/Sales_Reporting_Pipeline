#Gives the Functions to Load into SQL tables, truncate tables, Merge delta into tables,
#Populate the target table, Generates the Summary table

from db_connect import get_db_engine
import logging
from sqlalchemy import text
from config_reader import read_query

engine = get_db_engine()
#Function to Truncate tables.
def truncate_table(config_path, query_key):
    try:
        query = read_query(config_path, query_key)
        with engine.begin() as con:
            con.execute(text(query))
        logging.info(f"Successfully executed truncate: {query_key}")
    except Exception as e:
        logging.error(f"Error executing truncate `{query_key}`: {e}")
        exit(1)

#Funtion that loads the extracted csv into MySQL Database.
def load_into_table(df,table_name):
    try:
        df.to_sql(name=table_name,con=engine,if_exists='append',index=False)
        logging.info(f'Loaded {len(df)} records into {table_name}')
    except Exception as e:
        logging.error(f'Error loading into {table_name}: {e}')
        exit(1)

#Function to merge staging to target
def merge_staging_to_table(config_path, query_key):
    try:
        query = read_query(config_path, query_key)
        with engine.begin() as con:
            con.execute(text(query))
        logging.info(f"Merge query executed: {query_key}")
    except Exception as e:
        logging.error(f"Error executing merge query `{query_key}`: {e}")
        exit(1)


def populate_sales_target(config_path, query_key):
    try:
        query = read_query(config_path, query_key)
        with engine.begin() as con:
            con.execute(text(query))
        logging.info(f"Populated target table using: {query_key}")
    except Exception as e:
        logging.error(f"Error executing populate target `{query_key}`: {e}")
        exit(2)

def summarize_sales_data(config_path, query_key):
    try:
        query = read_query(config_path, query_key)
        with engine.begin() as con:
            con.execute(text(query))
        logging.info(f"Summary query executed: {query_key}")
    except Exception as e:
        logging.error(f"Error executing summary query `{query_key}`: {e}")
        exit(3)