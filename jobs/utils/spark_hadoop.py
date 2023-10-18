from pyspark.sql import SparkSession
from pyspark import SparkConf
from contextlib import contextmanager


"""
This file contains objects related to SparkSession and hadoopConfiguration 
(e.g. sparkContext, jvm, jsc, conf, fs), stored in SparkHadoop class.

SparkHadoop class does include Hive support, specified via get_spark_hadoop()
"""


class SparkHadoop:
    """
    Contains objects related to SparkSession and HadoopConfiguration.

    Properties:
    - spark: Current SparkSession.
    - jvm, jsc, conf, fs: Java-side objects.
    - hive_enabled: Enable Hive in session or not.
    """
    def __init__(self, spark: SparkSession, hive_enabled: bool = False):
        self.spark = spark
        self.sc = spark.sparkContext
        self.jvm = self.sc._jvm
        self.jsc = self.sc._jsc
        self.conf = self.jsc.hadoopConfiguration()
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.conf)
        self.hive_enabled = hive_enabled


@contextmanager
def get_spark_hadoop(app_name: str, hive_enabled: bool = False) -> SparkHadoop:
    """
    Get SparkSession and its SparkContext, JVM, JSC, 
    hadoopConfiguration, HDFS objects

    Args:
    - app_name: Name of Spark application.
    - hive_enabled: Enable Hive in session or not.

    Returns a SparkHadoop object of the Spark app with app_name.

    Usage:
    with get_spark_hadoop('my_app') as spark_hadoop:
        spark = spark_hadoop.spark
        ...
    """
    # Modify config for Spark
    conf = SparkConf()
    conf.set('fs.defaultFS', 'hdfs://0.0.0.0:9000/')
    conf.set('spark.sql.warehouse.dir', '/data_warehouse')
    conf.set('spark.hive.exec.dynamic.partition.mode', 'nonstrict')

    # Create SparkSession
    #TODO: Change to Postgres as DWH
    spark = SparkSession.builder.appName(app_name).config(conf = conf)
    if hive_enabled:
        spark = spark.enableHiveSupport().getOrCreate()
    else:
        spark = spark.getOrCreate()

    # Yield object for context manager 'with' statement
    try:
        yield SparkHadoop(spark, hive_enabled = hive_enabled)
    finally:
        spark.stop()