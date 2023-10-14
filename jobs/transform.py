from utils.spark_hadoop import get_spark_hadoop, SparkHadoop
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

    Note: In case of incorrect table name, it will not be executed.
    """
    with get_spark_hadoop(
        app_name = 'Transform raw data from data lake', 
        hive_enabled = True
    ) as spark_hadoop:
        spark = spark_hadoop.spark

        # Map tables to transformation function
        transform_map = {
            'orders': transform_orders,
            'order_items': transform_order_items,
            'order_payments': transform_order_payments,
            'order_reviews': transform_order_reviews,
            'customers': transform_customers
        }

        # Call respective transformation function for each executed table
        for tbl_name in tbl_names:
            transform_map.get(tbl_name, transform_nonexist)(spark)


def transform_orders(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming orders')

def transform_order_items(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_items')

def transform_order_payments(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_payments')

def transform_order_reviews(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_reviews')

def transform_customers(spark: SparkHadoop) -> None:
    print('TRANSFORM: Transforming order_customers')

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
            'order_reviews', 'customers'
        ]
    
    # Track execution time
    import time
    start_time = time.time()
    main(args.tbl_names)
    print('PROGRAM FINISHED: Took', time.time() - start_time, 'seconds')