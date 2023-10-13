from pyspark.sql import SparkSession
from assets.tables import TableCollection
from typing import List
import argparse


def main(tbl_names: List[str]):
    """
    Read input csv files for the specified table names. Induce new records and write them into HDFS.

    Arguments:
    - tbl_names: Name of tables to be executed (--tbl_names in CLI).
    Defaults to all tables available (specified in ./assets/tables.py)

    Exceptions:
    - If any tbl_name not available: ValueError.

    Usage: spark-submit ingest.py [--tbl_names tbl1 tbl2]
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
    
    # Ingest csv data into HDFS for specified tables
    for tbl_name in tbl_names:
        # Create table object:
        tbl = TableCollection().get_tbl(tbl_name)

        print('==================== PROCESSING', tbl_name, '====================')
        # Read input csv file 
        spark_df = spark.read.csv(
            tbl.path_csv, 
            header = True, 
            schema = tbl.schema_StructType
        )
        
        # Get latest records in data lake, return empty dataframe with schema if records not found
        if fs.exists(jvm.org.apache.hadoop.fs.Path(tbl.hdfs_dir + '/_SUCCESS')):
            df_latest = spark.read.parquet(tbl.hdfs_dir_uri, schema = tbl.schema_StructType)
        else:
            df_latest = spark.createDataFrame([], schema = tbl.schema_StructType)

        # Load new records into data lake if there are any
        new_records = spark_df.subtract(df_latest)
        if not new_records.isEmpty():
            new_records.write.mode('append').parquet(tbl.hdfs_dir_uri)
            print('>>>>> Updated', tbl_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tbl_names', type = str, nargs = '*')
    args = parser.parse_args()
    
    if not args.tbl_names:  # Execute on all tables if --tbl_names not provided
        args.tbl_names = TableCollection().get_tbl_names()
    
    import time
    start_time = time.time()
    main(args.tbl_names)
    print('>>>>> Execution time:', time.time() - start_time)