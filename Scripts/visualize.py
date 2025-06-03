#Helps Visualize the Summarized data for better Understanding.
import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import os
from extraction import extract_from_mysql

def visualize_daily_revenue():

    os.makedirs("output", exist_ok=True)
    sales_summary_df=extract_from_mysql("sales_summary")
    sales_summary_df['OrderDate']=pd.to_datetime(sales_summary_df['OrderDate'])
    plt.figure(figsize=[8,8])
    sns.lineplot(data=sales_summary_df, x='OrderDate', y='Total_Revenue', hue='Region', marker='o')
    # sns.barplot(data=sales_summary_df, x='OrderDate', y='Total_Revenue', hue='Region')
    plt.title('Daily Revenue by Region')
    plt.xlabel('Order Date')
    plt.ylabel('Revenue')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/daily_revenue.png", bbox_inches='tight')
    logging.info("Plot saved to output/daily_revenue.png")

