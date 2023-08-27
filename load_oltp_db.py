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
    - schemas/source_schema.sql
    """
    # Create OLTP schema
    try:
        # Connect to OLTP database, config and create cursor
        conn = pg.connect(
            database = 'olist',
            user = 'postgres',
            password = '1234'
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Read and execute script for creating OLTP schema
        with open(os.path.join(os.path.dirname(__file__), 'schemas/source_schema.sql'), 'r') as script:
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
        "olist_geolocation_dataset.csv": "geolocation",
        "olist_sellers_dataset.csv": "sellers",
        "olist_customers_dataset.csv": "customers",
        "product_category_name_translation.csv": "product_category_name_translation",
        "olist_products_dataset.csv": "products",
        "olist_orders_dataset.csv": "orders",
        "olist_order_reviews_dataset.csv": "order_reviews",
        "olist_order_payments_dataset.csv": "order_payments",
        "olist_order_items_dataset.csv": "order_items"
    }

    # Load data to tables from csv files
    with ZipFile(path_zip, 'r') as zip_file:
        for file_name in CSV_TO_TABLE_MAP:  # Must open csv files in this order to avoid violating FK constraints
                with zip_file.open(file_name, 'r') as csv_file:
                    """
                    Spark DataFrames are created via pd.DataFrame because Spark 
                    does not support reading from raw csv text
                    """
                    print('--------- Processing', file_name, '---------')
                    # Manually cast 'zip_code_prefix' to str to avoid wrong data type infer 
                    if file_name == 'olist_geolocation_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            dtype = {'geolocation_zip_code_prefix': str}
                        ))
                        spark_df = spark_df.groupby('geolocation_zip_code_prefix') \
                            .agg(
                                {
                                    'geolocation_lat': 'avg',
                                    'geolocation_lng': 'avg',
                                    'geolocation_city': 'first',
                                    'geolocation_state': 'first'
                                }
                            ).collect()
                        # Create new spark_df to write into Postgres from list of Rows 
                        # with aggregate columns renamed
                        spark_df = spark.createDataFrame(spark_df) \
                            .withColumnRenamed('avg(geolocation_lat)', 'geolocation_lat') \
                            .withColumnRenamed('avg(geolocation_lng)', 'geolocation_lng') \
                            .withColumnRenamed('first(geolocation_city)', 'geolocation_city') \
                            .withColumnRenamed('first(geolocation_state)', 'geolocation_state')
                        
                    elif file_name == 'olist_customers_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            usecols = ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix'],
                            dtype = {'customer_zip_code_prefix': str}
                        ))

                    elif file_name == 'olist_sellers_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            usecols = ['seller_id', 'seller_zip_code_prefix'],
                            dtype = {'seller_zip_code_prefix': str}
                        ))

                    else:
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file))

                    # Load data from csv into Postgres
                    spark_df.write.format('jdbc').mode('append') \
                        .option('url', 'jdbc:postgresql://localhost:5432/olist', ) \
                        .option('driver', 'org.postgresql.Driver') \
                        .option('dbtable', CSV_TO_TABLE_MAP[file_name]) \
                        .option('user', 'postgres') \
                        .option('password', '1234') \
                        .save()
                    ## TODO: Handle seller zip code non-existent in geolocation table

    
if __name__ == '__main__':
    main()