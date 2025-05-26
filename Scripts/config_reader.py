import configparser

def read_csv_path(config_path):
    config = configparser.ConfigParser()
    config.read('config_path')

    print("Sections found in INI file:", config.sections())

    return{
        'web_sales_data': config['CSV Path']['web_sales_data'],
        'pos_orders_data': config['CSV Path']['pos_orders_data']
    }