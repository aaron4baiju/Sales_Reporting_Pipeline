# Sales Reporting Pipeline

A mini ETL pipeline built using **Python**, **Pandas**, and **MySQL** to integrate and report sales data from both physical store (POS) and online (web) sources.

## Project Objective

Design and implement an automated ETL process that:
- Extracts sales data from two CSV sources: (`pos_orders`) and (`web_sales.csv`).
- Supports **full and incremental loads** using a `LastUpdated` column.
- Uses **Pandas** for staging and identifying deltas (new or updated records).
- Loads only changed records into a consolidated MySQL table: `sales_target`.
- Generates a **daily sales summary report** grouped by Region, Product, and Order Date.

## Data Sources

- **CSV Files**: `web_sales.csv` — Online sales.
                `pos_orders` — Physical store sales.

## Final Reporting Table

**Table Name**: `sales_target`  
Stores merged records with the following fields:
- OrderID
- CustomerID
- CustomerName
- Region
- Product
- Quantity
- Price
- Source
- Order Date
- LastUpdated

## Audit Tracking

The ETL pipeline maintains a **load audit tracker table** to record the last successful `LastUpdated` timestamp for each source. This supports incremental loading.

## 📊 Reporting Task

A SQL-based summary report that:
- Shows **daily total revenue** and **quantity sold**
- Grouped by **Region** and **Order Date**

## 📁 Folder Structure (Suggested)

Sales_Reporting_Pipeline/
│
├── Config/
|   ├── csvpath.ini
|   ├── db_config.ini
|   └── queries.ini
├── logs/
├── output/
|   └── daily_revenue.png
├── Scripts/
│   ├── config_reader.py
|   ├── db_connect.py
|   ├── extraction.py
|   ├── transform.py
│   ├── loading.py
│   ├── logger.py
|   ├── visualize.py
│   └── etl_main_etl.py
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
