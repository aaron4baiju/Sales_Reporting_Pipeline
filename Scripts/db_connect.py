#Establishes Database Connection by creating an SQL Engine from SQLAlchemy.

import configparser
import logging
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy.orm import sessionmaker

def get_db_engine():

    try:

        #Reading Database Credentials from db_config.ini
        config = configparser.ConfigParser()
        config.read(r"C:\Users\aaron\PycharmProjects\PythonProject\PythonProject\Sales_Reporting_Pipeline\config\db_config.ini")

        user=config['DB_Credentials']['user']
        password=config['DB_Credentials']['password']
        host=config['DB_Credentials']['host']
        port=config['DB_Credentials']['port']
        db_name=config['DB_Credentials']['db_name']

        #creating mysql engine from sqlalchemy
        password_encoded = quote_plus(password)
        engine=create_engine(f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{db_name}")
        logging.info("MySQL Engine Created.")
        return engine

    except Exception as e:
        logging.error("MySQL Engine Creation Error :" + str(e))
        exit(0)

