import unittest
import tracemalloc
from pyspark.sql import SparkSession, Row


class PySparkDFTest(unittest.TestCase):
    """
    Test for PySpark DataFrame actions
    """
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName('Test for PySpark DataFrame actions') \
            .master('local[1]') \
            .config('spark.driver.memory', '512M') \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pyspark_action(self):
        df = self.spark.createDataFrame([
            Row(letter='a', num=1),
            Row(letter='b', num=2)
        ])
        df.collect()


if __name__ == '__main__':
    # Ignored ResourceWarning from memory leaks, which is not correct
    # tested plain without unittest module
    unittest.main(warnings="ignore")