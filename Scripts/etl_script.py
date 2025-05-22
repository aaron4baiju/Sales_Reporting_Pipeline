import pandas as pd
from db_connect import get_db_engine
import logging

def extract_from_csv(file_path):

    df_web_sales=pd.read_csv("C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\web_sales_data.csv")
    logging.info("Data Extracted from CSV File to DataFrame.")
    return df_web_sales

def main():

    web_sales_file='C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\web_sales_data.csv'

    web_sales_data=extract_from_csv(web_sales_file)
