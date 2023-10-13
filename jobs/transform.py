from pyspark.sql import SparkSession
from typing import List
import argparse


def main():
    """
    """
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('Data load - source to olist database') \
        .getOrCreate()
    
    # Get FileSystem (specified by fs.defaultFS in core-site.xml - HDFS desired)
    sc = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc
    conf = jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)


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
    parser = argparse.ArgumentParser()
    parser.add_argument('--tbl_names', type = str, nargs = '*')
    args = parser.parse_args()
    
    if not args.tbl_names:  # Table to execute if --tbl_names not provided
        args.tbl_names = ['orders', 'order_items', 'order_payments', 'order_reviews', 'customers']

    import time
    start_time = time.time()
    main(args.tbl_names)
    print('>>>>> Execution time:', time.time() - start_time)