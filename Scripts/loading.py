import pandas as pd
from db_connect import get_db_engine
import logging

def load_into_mysql(df,table_name,if_exists=False):

    engine=get_db_engine()
    try:
        df.to_sql(name=table_name,con=engine,if_exists=if_exists,index=False)
        logging.info('Data successfully loaded in mysql')
        print(2)
    except Exception as e:
        logging.error(f'Error loading into MySQL: {e}')
        print(21)
        exit(1)