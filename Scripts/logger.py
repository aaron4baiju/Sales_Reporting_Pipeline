import logging
import os
import datetime

#Function to initialize a logger and formatting it.
def logger_setup(log_dir="logs"):
    os.makedirs("../logs", exist_ok=True)
    log_filename = os.path.join(datetime.datetime.now().strftime('logs/etl_pipeline_log_%Y-%m-%d.log'))

    # Configure the logger (only once)
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            filename=log_filename,
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode='a'  # Append mode
        )
    logging.info("Logger initialized.")

    print('Logger Setup Complete')