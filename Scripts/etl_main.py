#Main Function that carry's the Program Control.

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
from visualize import visualize_daily_revenue

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

    #DATE PARSING
    new_web['OrderDate'] = new_web['OrderDate'].apply(try_parse)
    new_web['LastUpdated'] = new_web['LastUpdated'].apply(try_parse)
    new_pos['OrderDate'] = new_pos['OrderDate'].apply(try_parse)
    new_pos['LastUpdated'] = new_pos['LastUpdated'].apply(try_parse)


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

        logging.info("Full Load Operation Begun.")
        truncate_table('Config/queries.ini', 'truncate_web_sales')
        logging.info('web_sales table truncated.')
        truncate_table('Config/queries.ini', 'truncate_pos_orders')
        logging.info('pos_orders table truncated.')

        load_into_table(new_web, 'web_sales')
        logging.info('Data loaded into web_sales table.')
        load_into_table(new_pos, 'pos_orders')
        logging.info('Data Loaded into pos_orders table.')
        logging.info('Full Load Complete.')

    #INCREMENTAL >> delta -> staging -> merging
    elif load_type == 'incremental':

        try:
            #Identify delta records
            logging.info("Incremental Load Operation Begun.")
            logging.info("Fetching Delta records...")
            delta_web = get_delta(new_web, old_web, timestamp_col='LastUpdated')
            logging.info('WEB_SALES_DELTA:')
            logging.info(delta_web)
            delta_pos = get_delta(new_pos, old_pos, timestamp_col='LastUpdated')
            logging.info('POS_ORDERS_DELTA')
            logging.info(delta_pos)

            #Staging and Merging
            if not delta_web.empty:
                truncate_table('Config/queries.ini', 'truncate_web_staging')
                logging.info('web_sales_staging table truncated.')
                load_into_table(delta_web, 'stg_web_sales')
                logging.info('Data loaded into web_sales_staging table.')
                merge_staging_to_table('Config/queries.ini', 'merge_stg_web_to_web_sales')
                logging.info('web_sales_staging merged into web_sales table.')

            if not delta_pos.empty:
                truncate_table('Config/queries.ini', 'truncate_pos_staging')
                logging.info('pos_orders_staging table truncated.')
                load_into_table(delta_pos, 'stg_pos_orders')
                logging.info('Data loaded into pos_orders_staging table.')
                merge_staging_to_table('Config/queries.ini', 'merge_stg_pos_to_pos_orders')
                logging.info('Data loaded into pos_orders_staging merged into pos_orders table.')

            logging.info("Incremental Load Complete.")

        except Exception as e:
            logging.error(f"Error performing incremental load operation: {e}")

    #Populating Target Table
    populate_sales_target('Config/queries.ini','populate_sales_target')
    logging.info("Populated sales target table.")

    #Summarize Sales Data using Sales Target
    truncate_table('Config/queries.ini', 'truncate_sales_summary')
    logging.info('Sales Summary table truncated.')
    summarize_sales_data('Config/queries.ini', 'summarize_sales_data')
    logging.info('Sales Summary table loaded.')

    #Visualize Daily Revenue through Sales Summary
    visualize_daily_revenue()
    logging.info('Sales Revenue table visualized.')

    print("Execution Completed.")

if __name__ == "__main__":
    main()
