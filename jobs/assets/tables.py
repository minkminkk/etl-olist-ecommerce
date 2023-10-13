from typing import List
from pyspark.sql.types import StructType, StructField, \
    StringType, ByteType, ShortType, IntegerType, FloatType, TimestampType, DateType
import os


# This file reorganize information of tables at Ingestion stage.
# - The TableCollection class stores information about all tables
# - The IngestionTable class stores table information, extracted via 
# TableCollection class by table name


class Table:
    """
    Stores information about a table ingested to HDFS.
    Should be instantiated by TableCollection class for data validation. 

    Properties:
    - tbl_name: Name of table.
    - csv_name: Name of the respective csv file.
    - schema_StructType: Table schema as StructType.
    - path_csv: Path of csv file in the local filesystem.
    - hdfs_dir: Path of HDFS directory of table.
    - hdfs_dir_uri: URI of HDFS directory of table.
    """
    def __init__(self, 
        tbl_name: str, 
        csv_name: str,
        schema_StructType: StructType
    ):
        self.tbl_name = tbl_name
        self.csv_name = csv_name
        self.schema_StructType = schema_StructType
        self.path_csv = 'file://' + os.path.join(os.getcwd(), 'data', self.csv_name)
        self.hdfs_dir = os.path.join('/data_lake', self.tbl_name)
        self.hdfs_dir_uri = 'hdfs://localhost:9000/' + self.hdfs_dir


class TableCollection:
    """
    Contains information of ingested tables in HDFS.
    Can be used to generate table.

    Methods:
    - get_tbl_names(): Get all table names in collection.
    - get_tbl(): Create a table at Ingestion stage.
    """
    _tbl_info = {
        'customers': {
            'csv_name': 'olist_customers_dataset.csv',
            'schema_StructType': StructType([
                StructField('customer_id', StringType(), False),
                StructField('customer_unique_id', StringType(), False),
                StructField('customer_zip_code_prefix', StringType(), False),
                StructField('customer_city', StringType(), False),
                StructField('customer_state', StringType(), False)
            ])
        },
        'geolocation': {
            'csv_name': 'olist_geolocation_dataset.csv',
            'schema_StructType': StructType([
                StructField('geolocation_zip_code_prefix', StringType(), False),
                StructField('geolocation_lat', FloatType(), False),
                StructField('geolocation_lng', FloatType(), False),
                StructField('geolocation_city', StringType(), False),
                StructField('geolocation_state', StringType(), False)
            ])
        },
        'orders': {
            'csv_name': 'olist_orders_dataset.csv',
            'schema_StructType': StructType([
                StructField('order_id', StringType(), False),
                StructField('customer_id', StringType(), False),
                StructField('order_status', StringType(), False),
                StructField('order_purchase_timestamp', TimestampType(), False),
                StructField('order_approved_at', TimestampType(), True),
                StructField('order_delivered_carrier_date', TimestampType(), True),
                StructField('order_delivered_customer_date', TimestampType(), True),
                StructField('order_estimated_delivery_date', DateType(), False)
            ])
        },
        'order_items': {
            'csv_name': 'olist_order_items_dataset.csv',
            'schema_StructType': StructType([
                StructField('order_id', StringType(), False),
                StructField('order_item_id', StringType(), False),
                StructField('product_id', StringType(), False),
                StructField('seller_id', StringType(), False),
                StructField('shipping_limit_date', TimestampType(), False),
                StructField('price', FloatType(), False),
                StructField('freight_value', FloatType(), False)
            ])
        },
        'order_payments': {
            'csv_name': 'olist_order_payments_dataset.csv',
            'schema_StructType': StructType([
                StructField('order_id', StringType(), False),
                StructField('payment_sequential', ByteType(), False),   # for cases of multiple payment methods per order
                StructField('payment_type', StringType(), False),
                StructField('payment_installments', ByteType(), False),
                StructField('payment_value', FloatType(), False)
            ])
        },
        'order_reviews': {
            'csv_name': 'olist_order_reviews_dataset.csv',
            'schema_StructType': StructType([
                StructField('review_id', StringType(), False),
                StructField('order_id', StringType(), False),
                StructField('review_score', ByteType(), False),   # from 1 to 5
                StructField('review_comment_title', StringType(), True),
                StructField('review_comment_message', StringType(), True),
                StructField('review_creation_date', DateType(), False),
                StructField('review_answer_timestamp', TimestampType(), False)
            ])
        },
        'products': {
            'csv_name': 'olist_products_dataset.csv',
            'schema_StructType': StructType([
                StructField('product_id', StringType(), False),
                StructField('product_category_name', StringType(), True),
                StructField('product_name_length', ByteType(), False),
                StructField('product_description_length', ShortType(), True), 
                StructField('product_photos_qty', ByteType(), True),
                StructField('product_weight_g', IntegerType(), False),
                StructField('product_length_cm', ShortType(), False),
                StructField('product_height_cm', ShortType(), False),
                StructField('product_width_cm', ShortType(), False)
            ])
        },
        'product_category_name_translation': {
            'csv_name': 'product_category_name_translation.csv',
            'schema_StructType': StructType([
                StructField('product_category_name', StringType(), False),
                StructField('product_category_name_english', StringType(), False)
            ])
        },
        'sellers': {
            'csv_name': 'olist_sellers_dataset.csv',
            'schema_StructType': StructType([
                StructField('seller_id', StringType(), False),
                StructField('seller_zip_code_prefix', StringType(), False),
                StructField('seller_city', StringType(), False),
                StructField('seller_state', StringType(), False)
            ])
        }
    }
    
    def get_tbl_names(self) -> List[str]:
        """
        Returns names of tables in collection
        """
        return list(TableCollection._tbl_info.keys())

    def get_tbl(self, tbl_name: str) -> Table:
        """
        Gets IngestionTable object by table name
        """
        # Table name validation
        if tbl_name not in TableCollection().get_tbl_names():
            raise ValueError('Table name must be within allowed values')
        
        return Table(
            tbl_name = tbl_name,
            csv_name = TableCollection._tbl_info[tbl_name]['csv_name'],
            schema_StructType = TableCollection._tbl_info[tbl_name]['schema_StructType']
        )