import pandas as pd
from db_connect import get_db_engine
import logging

def extract_from_csv(file_path):

    df_web_sales=pd.read_csv(file_path)
    logging.info("Data Extracted from CSV File to DataFrame.")
    print(1)
    return df_web_sales


def main():

    web_sales_file='C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\web_sales_data.csv'
    pos_order_file='C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\pos_orders_data.csv'

    web_sales_data=extract_from_csv(web_sales_file)
    pos_order_data=extract_from_csv(pos_order_file)

    print(web_sales_data.head())
    print(pos_order_data.head())



if __name__ == '__main__':
    main()