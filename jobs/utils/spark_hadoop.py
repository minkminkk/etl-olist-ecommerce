from pyspark.sql import SparkSession
from contextlib import contextmanager


class SparkHadoop:
    """
    Contains objects related to SparkSession and HadoopConfiguration.

    Properties:
    - spark: Current SparkSession.
    - jvm, jsc, conf, fs: Java-side objects.
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

    Returns a SparkHDFS object of the Spark app with app_name.

    Usage:
    with get_spark_session_hdfs('my_app') as spark, fs:
        ...
    """
    # Create SparkSession
    spark = SparkSession.builder \
        .appName(app_name)
    
    if hive_enabled:
        spark = spark.enableHiveSupport().getOrCreate()
    else:
        spark = spark.getOrCreate()

    # Yield object for context manager 'with' statement
    try:
        yield SparkHadoop(spark, hive_enabled = hive_enabled)
    finally:
        spark.stop()