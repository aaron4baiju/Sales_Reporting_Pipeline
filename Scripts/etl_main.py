import sys
from extraction import extract_from_csv
from extraction import extract_from_mysql
from transform import get_delta
from loading import load_into_mysql
from logger import logger_setup
from config_reader import read_csv_path

logger_setup()
def main():
    csv_path=read_csv_path('Config/csvpath.ini')
    web_file =csv_path['web_sales_data']
    pos_file =csv_path['pos_orders_data']

    # Extract new data
    new_web = extract_from_csv(web_file)
    new_pos = extract_from_csv(pos_file)

    # Extract old data from MySQL
    old_web = extract_from_mysql("web_sales")
    old_pos = extract_from_mysql("pos_orders")

    # Default to incremental if no argument is passed
    if len(sys.argv) > 1:
        load_type = sys.argv[1].strip().lower()
        if load_type not in ['full', 'incremental']:
            print("Invalid load type. Defaulting to incremental.")
            load_type = 'incremental'
    else:
        print("No load type specified. Defaulting to incremental.")
        load_type = 'incremental'

    # Delta detection
    delta_web = get_delta(new_web, old_web, load_type=load_type)
    delta_pos = get_delta(new_pos, old_pos, load_type=load_type)

    # Load delta
    if not delta_web.empty:
        load_into_mysql(delta_web, "web_sales")
    if not delta_pos.empty:
        load_into_mysql(delta_pos, "pos_orders")

if __name__ == "__main__":
    main()