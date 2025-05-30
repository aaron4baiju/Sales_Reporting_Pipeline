import sys
import pandas as pd
import logging
from extraction import extract_from_csv
from extraction import extract_from_mysql
from transform import get_delta
from loading import load_into_table,truncate_table,merge_staging_to_table,populate_sales_target,summarize_sales_data
from logger import logger_setup
from config_reader import read_csv_path
from dateutil.parser import parse

def try_parse(val):
    """Safely parse dates using dateutil, returning NaT if invalid."""
    try:
        return parse(val)
    except Exception:
        return pd.NaT


def main():
    logger_setup()
    csv_path=read_csv_path('Config/csvpath.ini')
    web_file =csv_path['web_sales_data']
    pos_file =csv_path['pos_orders_data']

    # Extract new data
    new_web = extract_from_csv(web_file)
    new_pos = extract_from_csv(pos_file)

    # Extract old data from MySQL
    old_web = extract_from_mysql("web_sales")
    old_pos = extract_from_mysql("pos_orders")

    # Log original dtypes and samples
    logging.info(f"Original dtypes for new_web: {new_web[['OrderDate', 'LastUpdated']].dtypes}")
    logging.info(f"Original dtypes for new_pos: {new_pos[['OrderDate', 'LastUpdated']].dtypes}")
    logging.info("Sample of original new_web dates:\n" + str(new_web[['OrderDate', 'LastUpdated']].head()))
    logging.info("Sample of original new_pos dates:\n" + str(new_pos[['OrderDate', 'LastUpdated']].head()))

    #DATE PARSING
    new_web['OrderDate'] = new_web['OrderDate'].apply(try_parse)
    new_web['LastUpdated'] = new_web['LastUpdated'].apply(try_parse)
    new_pos['OrderDate'] = new_pos['OrderDate'].apply(try_parse)
    new_pos['LastUpdated'] = new_pos['LastUpdated'].apply(try_parse)

    # Log parsed dtype and samples
    logging.info(f"Dtypes for new_web AFTER conversion: {new_web[['OrderDate', 'LastUpdated']].dtypes}")
    logging.info(f"Dtypes for new_pos AFTER conversion: {new_pos[['OrderDate', 'LastUpdated']].dtypes}")
    logging.info("Sample of new_web dates AFTER conversion:\n" + str(new_web[['OrderDate', 'LastUpdated']].head()))
    logging.info("Sample of new_pos dates AFTER conversion:\n" + str(new_pos[['OrderDate', 'LastUpdated']].head()))
    logging.info(
        "Null counts in new_web after conversion:\n" + str(new_web[['OrderDate', 'LastUpdated']].isnull().sum()))
    logging.info(
        "Null counts in new_pos after conversion:\n" + str(new_pos[['OrderDate', 'LastUpdated']].isnull().sum()))


    # Default to incremental if no argument is passed
    if len(sys.argv) > 1:
        load_type = sys.argv[1].strip().lower()
        if load_type not in ['full', 'incremental']:
            print("Invalid load type. Defaulting to incremental.")
            load_type = 'incremental'
    else:
        load_type = 'incremental'

    #FULL >> truncate -> Load
    if load_type == 'full':

        truncate_table('Config/queries.ini', 'truncate_web_sales')
        truncate_table('Config/queries.ini', 'truncate_pos_orders')

        load_into_table(new_web, 'web_sales')
        load_into_table(new_pos, 'pos_orders')

    #INCREMENTAL >> delta -> staging -> merging
    elif load_type == 'incremental':

        try:
            #Identify delta records
            delta_web = get_delta(new_web, old_web, timestamp_col='LastUpdated')
            print('WEB_SALES_DELTA')
            print(delta_web)
            delta_pos = get_delta(new_pos, old_pos, timestamp_col='LastUpdated')
            print('POS_ORDERS_DELTA')
            print(delta_pos)

            #Staging and Merging
            if not delta_web.empty:
                truncate_table('Config/queries.ini', 'truncate_web_staging')
                load_into_table(delta_web, 'stg_web_sales')
                merge_staging_to_table('Config/queries.ini', 'merge_stg_web_to_web_sales')

            if not delta_pos.empty:
                truncate_table('Config/queries.ini', 'truncate_pos_staging')
                load_into_table(delta_pos, 'stg_pos_orders')
                merge_staging_to_table('Config/queries.ini', 'merge_stg_pos_to_pos_orders')

        except Exception as e:
            print(e)

    #Populating Target Table
    populate_sales_target('Config/queries.ini','populate_sales_target')

    #Summarize Sales Data using Sales Target
    truncate_table('Config/queries.ini', 'truncate_sales_summary')
    summarize_sales_data('Config/queries.ini', 'summarize_sales_data')

if __name__ == "__main__":
    main()
