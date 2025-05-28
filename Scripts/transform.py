import pandas as pd
from db_connect import get_db_engine
import logging

def get_delta(new_df, old_df, load_type = 'incremental', timestamp_col='LastUpdated', source=None):
    # Convert columns to datetime if needed
    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col], dayfirst=True, errors='coerce')
    old_df[timestamp_col] = pd.to_datetime(old_df[timestamp_col], dayfirst=True, errors='coerce')

    if source:
        new_df['source'] = source

    # #For FULL Load
    # if load_type.lower() == 'full':
    #     print(f"Performing FULL load for {source}...")
    #     return new_df

    #For INCR. LOAD
    elif load_type.lower() == 'incremental':
        print(f"Performing INCREMENTAL load for {source}...")

        if old_df.empty:
            logging.info("No existing data found — full load will be performed.")
            return new_df

        latest = old_df[timestamp_col].max()
        delta_df = new_df[new_df[timestamp_col] > latest]
        return delta_df

    # Filter new/changed records
    latest_timestamp = old_df[timestamp_col].max()
    delta_df = new_df[new_df[timestamp_col] > latest_timestamp]
    logging.info(f"{len(delta_df)} new/updated records found.")
    return delta_df




