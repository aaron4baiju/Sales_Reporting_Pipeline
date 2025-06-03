#Read the config file into the main scripts.

import configparser
import os
import logging

def read_csv_path(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)

    if not os.path.exists(config_path):
        logging.error(f"Config file {config_path} does not exist.")
        raise FileNotFoundError(f"{config_path} not found.")

    config.read(config_path)
    logging.info(f"Sections found in INI file: {config.sections()}")

    return {
        'web_sales_data': config['CSV Path']['web_sales_data'],
        'pos_orders_data': config['CSV Path']['pos_orders_data']
    }



def read_query(config_path, query_key):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['SQL'][query_key]