from extraction import extract_from_csv
from loading import load_into_mysql

def main():
    web_sales_file='C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\web_sales_data.csv'
    pos_order_file='C:\\Users\\aaron\\PycharmProjects\\PythonProject\\PythonProject\\Sales_Reporting_Pipeline\\pos_orders_data.csv'

    web_sales_data=extract_from_csv(web_sales_file)
    pos_order_data=extract_from_csv(pos_order_file)
    print(3)
    load_into_mysql(web_sales_data,'web_sales',if_exists='replace')
    load_into_mysql(pos_order_data,'pos_orders',if_exists='replace')
    print(4)

if __name__ == '__main__':
    main()