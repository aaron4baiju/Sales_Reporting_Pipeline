import pandas as pd
from db_connect import get_db_engine
import logging

def get_delta(new_df, old_df, timestamp_col='LastUpdated'):
    # Convert columns to datetime if needed
#     new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col], dayfirst=True, errors='coerce')
#     old_df[timestamp_col] = pd.to_datetime(old_df[timestamp_col], dayfirst=True, errors='coerce')

    print("Max timestamp in old_df:", old_df[timestamp_col].max())
    print("Min timestamp in old_df:", old_df[timestamp_col].min())
    print("Max timestamp in new_df:", new_df[timestamp_col].max())
    print("Min timestamp in new_df:", new_df[timestamp_col].min())
    print("Number of new_df rows > old_df max timestamp:",
          (new_df[timestamp_col] > old_df[timestamp_col].max()).sum())

    # df1 = new_df[new_df[timestamp_col]>old_df[timestamp_col].max()]
    # return df1

    # For INCR. LOAD
    print(f"Performing INCREMENTAL load")
    #Delta LOGIC
    if old_df.empty:
        logging.info("No existing data found — full load will be performed.")
        return new_df

    else:
        latest = old_df[timestamp_col].max()
        logging.info(f"Latest timestamp in old data: {latest}")

        delta_df = new_df[new_df[timestamp_col] > latest]
        logging.info(f'Delta Records Identified: {len(delta_df)}')

        return delta_df


