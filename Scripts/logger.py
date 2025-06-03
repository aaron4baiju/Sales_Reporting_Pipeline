#Initiazes the Logger to store Day-to-Day Logs.

import logging
import os
import datetime

#Function to initialize a logger and formatting it.
def logger_setup(log_dir="logs"):
    os.makedirs("../logs", exist_ok=True)
    log_filename = os.path.join(log_dir, f"etl_pipeline_log_{datetime.datetime.now():%Y-%m-%d}.log")

    # Clear all previous handlers to avoid duplicate or no logs
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure the logger (only once)
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode='a'  # Append mode
        )
    logging.info("\n\n\n\n\n\nLogger initialized.")



