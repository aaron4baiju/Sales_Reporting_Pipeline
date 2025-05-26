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

    # Delta detection
    delta_web = get_delta(new_web, old_web)
    delta_pos = get_delta(new_pos, old_pos)

    # Load delta
    if not delta_web.empty:
        load_into_mysql(delta_web, "web_sales")
    if not delta_pos.empty:
        load_into_mysql(delta_pos, "pos_orders")

if __name__ == "__main__":
    main()