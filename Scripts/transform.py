import pandas as pd
from db_connect import get_db_engine
import logging

def get_delta(new_df, old_df, timestamp_col='LastUpdated'):
    if old_df.empty:
        logging.info("No existing data found — full load will be performed.")
        return new_df

    # Convert columns to datetime if needed
    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])
    old_df[timestamp_col] = pd.to_datetime(old_df[timestamp_col])

    # Filter new/changed records
    latest_timestamp = old_df[timestamp_col].max()
    delta_df = new_df[new_df[timestamp_col] > latest_timestamp]

    logging.info(f"{len(delta_df)} new/updated records found.")
    return delta_df