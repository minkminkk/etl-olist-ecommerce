from pyspark.sql import SparkSession
import pandas as pd
import psycopg2 as pg
import os
from zipfile import ZipFile


def main():
    """
    Creates schema & tables and load csv data onto OLTP database

    Input files: 
    - olist-data.zip
    - schemas/oltp_schema.sql
    """
    # Create OLTP schema
    try:
        # Connect to OLTP database, config and create cursor
        conn = pg.connect(
            database = 'olist-ecommerce',
            user = 'postgres',
            password = '1234'
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Read and execute script for creating OLTP schema
        with open(os.path.join(os.path.dirname(__file__), 'schemas/oltp_schema.sql'), 'r') as script:
            script = script.read()
            cur.execute(script)
    except:
        print('Error occurred')
    finally:
        conn.close()

    # Create SparkSession
    spark = SparkSession.builder \
        .config('spark.jars', '/usr/local/postgresql-42.6.0.jar') \
        .master('local') \
        .appName('Data load - source to OLTP database') \
        .getOrCreate()
    
    # Load data from each csv file into tables in OLTP database
    path_wd = os.path.dirname(__file__)
    path_zip = os.path.join(path_wd, 'olist-data.zip')
    
    # Map filenames onto respective PostgreSQL tables
    CSV_TO_TABLE_MAP = {
        "olist_orders_dataset.csv": "oltp_schema.orders",
        "olist_order_items_dataset.csv": "oltp_schema.order_items",
        "olist_order_payments_dataset.csv": "oltp_schema.order_payments",
        "olist_order_reviews_dataset.csv": "oltp_schema.order_reviews",
        "olist_customers_dataset.csv": "oltp_schema.customers",
        "olist_products_dataset.csv": "oltp_schema.products",
        "product_category_name_translation.csv": "oltp_schema.product_category_name_translation",
        "olist_sellers_dataset.csv": "oltp_schema.sellers",
        "olist_geolocation_dataset.csv": "oltp_schema.geolocation"
    }

    # File list: To avoid FK constraint violation when loading data
    FILE_LIST = [
        "olist_geolocation_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_customers_dataset.csv",
        "product_category_name_translation.csv",
        "olist_products_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_items_dataset.csv"
    ]

    # Load data to tables from csv files
    with ZipFile(path_zip, 'r') as zip_file:
        for file_name in FILE_LIST:
                with zip_file.open(file_name, 'r') as csv_file:
                    # Create DataFrame via pd.DataFrame because spark does not support
                    # reading from raw csv string
                    spark_df = spark.createDataFrame(pd.read_csv(csv_file))
                    
                    # Remove redundant columns in customers & sellers table
                    if file_name == 'olist_customers_dataset.csv':
                        spark_df = spark_df.drop('customer_city', 'customer_state')
                    if file_name == 'olist_sellers_dataset.csv':
                        spark_df = spark_df.drop('seller_city', 'seller_state')

                    # Load data from csv into PostgreSQL
                    spark_df.write.format('jdbc').mode('append') \
                        .option('url', 'jdbc:postgresql://localhost:5432/olist-ecommerce', ) \
                        .option('driver', 'org.postgresql.Driver') \
                        .option('dbtable', CSV_TO_TABLE_MAP[file_name]) \
                        .option('batchsize', 1000) \
                        .option('user', 'postgres') \
                        .option('password', '1234') \
                        .save()
                    
                    ## TODO: Analyze data (geolocation table - zip code prefix) 

    
if __name__ == '__main__':
    main()