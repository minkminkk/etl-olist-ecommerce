import unittest
import tracemalloc
from pyspark.sql import SparkSession, Row


class PySparkDFTest(unittest.TestCase):
    """
    Test for PySpark DataFrame actions
    """
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName('Test for PySpark DataFrame actions') \
            .master('local[1]') \
            .config('spark.driver.memory', '512M') \
            .getOrCreate()
    
    def tearDown(self):
        self.spark.stop()

    def test_pyspark_action(self):
        df = self.spark.createDataFrame([
            Row(letter='a', num=1),
            Row(letter='b', num=2)
        ])
        self.assertEqual(df.collect(), [Row(letter='a', num=1), Row(letter='b', num=2)])


if __name__ == '__main__':
    # Ignored ResourceWarning from memory leaks, which is not correct
    # tested plain without unittest module
    unittest.main(warnings="ignore")