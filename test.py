from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import timeit


def main():
    # spark = SparkSession.builder.getOrCreate()
    # spark_df = spark.read.csv('./data/olist_order_reviews_dataset.csv', 
    #     header = True,
    #     inferSchema = True
    # )
    # spark_df.printSchema()
    # spark_df.show(5)

    df = pd.read_csv('./data/olist_orders_dataset.csv')
    print(df.info())
    # print(df.head())

if __name__ == '__main__':
    print('Execution time:', timeit.timeit(main, number=1))