from pyspark.sql import SparkSession
from utils import schemas
from typing import List
from utils import misc
import os
import datetime
import argparse


def main(tbl_names: List[str], ingestion_date: str):
    """
    Read input csv files for the specified table names. Induce new records and write them into HDFS.

    Arguments:
    - tbl_names: Name of tables to be executed.
    Input all available tables (specified in ./utils/misc.py) if not provided.
    - ingestion_date: Date where we want to update data.
    Input execution date if not provided.

    Exceptions:
    - If any tbl_name not available: ValueError.
    - If ingestion_date later than the execution date: ValueError.

    Usage: spark-submit ingest.py
    """
    # Table name validation
    for tbl_name in tbl_names:
        if tbl_name not in misc.get_available_tbl_names():
            raise ValueError('Table names must be within allowed values: misc.get_available_tbl_names()')
    
    # Date validation
    date_format = '%Y-%m-%d'    # refer to datetime format codes
    ingestion_date = datetime.datetime.strptime(ingestion_date, date_format)  # raises ValueError if cannot parse date
    if ingestion_date.date() > datetime.date.today():
        raise ValueError('Cannot ingest data from the future')
    
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

    # Working directories
    path_wd = os.path.dirname(os.path.dirname(__file__))
    path_data = os.path.join(path_wd, 'data')
    
    # Ingest csv data into HDFS for specified tables
    for tbl_name in tbl_names:
        # TODO: Implement so able to ingest by selected date
        # HDFS directory for each table
        path_csv = 'file://' + os.path.join(path_data, misc.get_csvname_from_tblname(tbl_name))   # to specify files in local storage
        tbl_schema = schemas.IngestionSchema(tbl_name).as_StructType()
        path_hdfs_dir = '/data_lake/' + tbl_name
        path_hdfs_dir_uri = 'hdfs://localhost:9000' + path_hdfs_dir

        print('==================== PROCESSING', tbl_name, '====================')

        # Read input csv file
        spark_df = spark.read.csv(path_csv, header = True, schema = tbl_schema)
        
        # Get latest records in data lake, return empty dataframe with schema if records not found
        if fs.exists(jvm.org.apache.hadoop.fs.Path(path_hdfs_dir + '/_SUCCESS')):
            df_latest = spark.read.parquet(path_hdfs_dir_uri, header = True, schema = tbl_schema)
        else:
            df_latest = spark.createDataFrame(sc.emptyRDD(), schema = tbl_schema)

        # Load new records into data lake if there are any
        new_records = spark_df.subtract(df_latest)
        if not new_records.isEmpty():
            new_records.write.mode('append').parquet(path_hdfs_dir_uri)
            print('>>>>> Updated', tbl_name)
        else:
            print('>>>>>', tbl_name, 'already up to date')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tbl_names', type = str, nargs = '*')
    parser.add_argument('--ingestion_date', type = str)
    args = parser.parse_args()
    
    if not args.tbl_names:  # Execute on all tables if --tbl_names not provided
        args.tbl_names = misc.get_available_tbl_names()
    if not args.ingestion_date: # Execute on current date if --ingestion_date not provided
        args.ingestion_date = datetime.date.today().strftime('%Y-%m-%d')
    
    main(args.tbl_names, args.ingestion_date)