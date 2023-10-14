from utils.spark_hadoop import get_spark_hadoop
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
    """
    with get_spark_hadoop(
        app_name = 'Transform raw data', 
        hive_enabled = True
    ) as spark_hadoop:
        pass


def transform_orders():
    pass


def transform_order_items():
    pass


def transform_order_payments():
    pass


def transform_order_reviews():
    pass


def transform_customers():
    pass


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
    print('>>>>> Execution time:', time.time() - start_time)