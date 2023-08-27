from zipfile import ZipFile
from pyspark.sql import SparkSession
import pandas as pd
import os


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .config('spark.jars', '/usr/local/postgresql-42.6.0.jar') \
        .appName('Data load - source to OLTP database') \
        .getOrCreate()
    
    # Generate gateway to JVM and proxy into JSC
    sc = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc
    conf = jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    
    # TODO: Get latest record in datalake


    # Load data from each csv file into tables in OLTP database
    path_wd = os.path.dirname(__file__)
    path_zip = os.path.join(path_wd, 'olist-data.zip')
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
        for file_name in zip_file.namelist():
                with zip_file.open(file_name, 'r') as csv_file:
                    # Manually cast data types to some column to avoid wrong inferring
                    if file_name == 'olist_geolocation_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            dtype = {'geolocation_zip_code_prefix': str}
                        ))
                        
                    elif file_name == 'olist_customers_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            dtype = {'customer_zip_code_prefix': str}
                        ))

                    elif file_name == 'olist_sellers_dataset.csv':
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file,
                            dtype = {'seller_zip_code_prefix': str}
                        ))

                    else:
                        spark_df = spark.createDataFrame(pd.read_csv(csv_file))

                    # Load data from csv into Postgres
                    spark_df.write \
                        .parquet('hdfs://0.0.0.0:9000/data_lake' + CSV_TO_TABLE_MAP[file_name]) \
                        .mode('append') 
                    # TODO: Solve right to write data onto HDFS


if __name__ == '__main__':
    main()