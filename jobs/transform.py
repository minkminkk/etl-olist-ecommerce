from utils.spark_hadoop import get_spark_hadoop, SparkHadoop
from utils.tables import TableCollection
from pyspark.sql.functions import col, year, month, dayofmonth 
from typing import List
import argparse


def main(tbl_names: List[str]):
    """
    Transform tables in HDFS for DWH loading.

    Arguments:
    - tbl_names: Name of tables to be executed (--tbl_names in CLI).
    
    Default tables:
    - customers
    - orders
    - order_items
    - order_products
    - order_reviews

    Usage: spark-submit transform.py [--tbl_names tbl1 tbl2]

    Note: Tables with incorrect names will not be executed.
    """
    with get_spark_hadoop(
        app_name = 'Transform raw data from data lake', 
        hive_enabled = True
    ) as spark_hadoop:
        # Map tables to transformation function and necessary tables
        transform_map = {
            'orders': transform_orders,
            'order_items': transform_order_items,
            'order_payments': transform_order_payments,
            'order_reviews': transform_order_reviews,
            'customers': transform_customers,
            'products': transform_products
        }

        # Create temp view of tables - temporary code block
        for tbl_name in tbl_names:
            tbl = TableCollection().get_tbl(tbl_name)
            df = spark_hadoop.spark.read.parquet(tbl.hdfs_dir_uri)
            df.createOrReplaceTempView(tbl_name)

        # Call respective transformation function for each executed table
        for tbl_name in tbl_names:
            transform_map.get(tbl_name, transform_nonexist)(spark_hadoop.spark)

# TODO: Implement loading into Postgres 
def transform_orders(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming orders')
    
    fct_orders = spark.sql('SELECT * FROM orders ORDER BY order_purchase_timestamp') \
        .withColumn('year', year(col('order_purchase_timestamp'))) \
        .withColumn('month', month(col('order_purchase_timestamp'))) \
        .withColumn('day', dayofmonth(col('order_purchase_timestamp')))


def transform_order_items(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_items')


def transform_order_payments(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_payments')


def transform_order_reviews(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_reviews')


def transform_customers(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_customers')


def transform_products(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming products')


def transform_nonexist(spark: SparkHadoop) -> None:
    print('WARNING: Table not exist therefore it will not be further transformed')


if __name__ == '__main__':
    # Parse CLI arguments into args
    parser = argparse.ArgumentParser()
    parser.add_argument('--tbl_names', type = str, nargs = '*')
    args = parser.parse_args()
    
    # Execute on all tables if --tbl_names not provided
    if not args.tbl_names:
        args.tbl_names = [
            'orders', 'order_items', 'order_payments', 
            'order_reviews', 'customers', 'products', 'product_category_name_translation'
        ]
    
    # Track execution time
    import time
    start_time = time.time()
    main(args.tbl_names)
    print('PROGRAM FINISHED: Took', time.time() - start_time, 'seconds')