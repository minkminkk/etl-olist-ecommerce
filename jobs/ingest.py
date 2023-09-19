from pyspark.sql import SparkSession
from jobs.utils import schemas
from typing import List, Union
import utils.utils as utils
import os
import argparse

def main(tblname: Union[str, List[str]], ingest_date: str):
    # Process arguments

    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName('Data load - source to olist database') \
        .getOrCreate()
    
    # Generate FileSystem
    sc = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc
    conf = jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

    # Write each csv file into HDFS
    path_wd = os.path.dirname(os.path.dirname(__file__))
    path_data = os.path.join(path_wd, 'data')
    
    for file_name in os.listdir(path_data):
        # HDFS directory for each table
        path_csv = 'file://' + os.path.join(path_data, file_name)   # to specify files in local storage
        tbl_name = utils.get_ingestion_tblname_from_csvname(file_name)
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
    import time
    start_time = time.time()
    main()
    print('Execution time:', time.time() - start_time)