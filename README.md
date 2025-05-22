# Sales Reporting Pipeline

A mini ETL pipeline built using **Python**, **Pandas**, and **MySQL** to integrate and report sales data from both physical store (POS) and online (web) sources.

## Project Objective

Design and implement an automated ETL process that:
- Extracts sales data from two sources: a MySQL table (`pos_orders`) and a CSV file (`web_sales.csv`).
- Supports **full and incremental loads** using a `LastUpdated` column.
- Uses **Pandas** for staging and identifying deltas (new or updated records).
- Loads only changed records into a consolidated MySQL table: `sales_fact`.
- Generates a **daily sales summary report** grouped by Region and Order Date.

## Data Sources

- **CSV Files**: `web_sales.csv` — Online sales.
                `pos_orders` — Physical store sales.

## Final Reporting Table

**Table Name**: `sales_fact`  
Stores merged records with the following fields:
- Customer Info
- Region
- Product
- Quantity
- Price
- Order Date
- Data Source
- LastUpdated

## 🛠️ Audit Tracking

The ETL pipeline maintains a **load audit tracker table** to record the last successful `LastUpdated` timestamp for each source. This supports incremental loading.

## 📊 Reporting Task

A SQL-based summary report that:
- Shows **daily total revenue** and **quantity sold**
- Grouped by **Region** and **Order Date**

## ✅ Task Breakdown

### Level 1: Environment Setup
- Set up the MySQL database and create necessary tables.
- Install required Python packages (e.g., `pandas`, `sqlalchemy`, `pymysql`, `configparser`).

### Level 2: Data Extraction
- Extract from:
  - MySQL table (`pos_orders`)
  - CSV file (`web_sales.csv`)

### Level 3: Delta Identification
- Use `LastUpdated` column to detect new/updated rows compared to previous runs.

### Level 4: Merge & Transform
- Combine data into a single DataFrame.
- Add data source identifier (POS / WEB).

### Level 5: Data Load
- Load delta records into `sales_fact` table.

### Level 6: Reporting
- Generate a SQL query/report summarizing daily sales by Region and Order Date.

## 📁 Folder Structure (Suggested)

Sales_Reporting_Pipeline/
│
├── scripts/
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ └── main_etl.py
│
├── data/
│ └── web_sales.csv
│
├── config/
│ └── db_config.ini
│
├── sql/
│ ├── create_tables.sql
│ └── daily_summary_report.sql
│
├── requirements.txt
└── README.md
