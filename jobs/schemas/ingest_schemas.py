#TODO: StructType for csv read into tables

from typing import Literal, get_args
from pyspark.sql.types import StructType, StructField, \
    StringType, ByteType, ShortType, IntegerType, FloatType, TimestampType, DateType


"""
Define schemas for tables at Ingestion stage as Spark StructType
for reading csv files
"""
# The available table names
AvailableTblnames = Literal[
    'customers', 
    'geolocation', 
    'orders', 
    'order_items',
    'order_payments', 
    'order_reviews', 
    'products', 
    'product_category_name_translation',
    'sellers' 
]

class IngestionSchema:
    """
    Class to generate schema by given table name

    Argument:
    - tblname: Name of table to get schema. Raise ValueError if table name does not exist.

    Method:
    - as_StructType(): Returns schema of the specified table as Spark's StructType.
    """
    _schemas_StructType = {
        'customers': StructType([
            StructField('customer_id', StringType(), False),
            StructField('customer_unique_id', StringType(), False),
            StructField('customer_zip_code_prefix', StringType(), False),
            StructField('customer_city', StringType(), False),
            StructField('customer_state', StringType(), False)
        ]),
        'geolocation': StructType([
            StructField('geolocation_zip_code_prefix', StringType(), False),
            StructField('geolocation_lat', FloatType(), False),
            StructField('geolocation_lng', FloatType(), False),
            StructField('geolocation_city', StringType(), False),
            StructField('geolocation_state', StringType(), False)
        ]),
        'orders': StructType([
            StructField('order_id', StringType(), False),
            StructField('customer_id', StringType(), False),
            StructField('order_status', StringType(), False),
            StructField('order_purchase_timestamp', TimestampType(), False),
            StructField('order_approved_at', TimestampType(), True),
            StructField('order_delivered_carrier_date', TimestampType(), True),
            StructField('order_delivered_customer_date', TimestampType(), True),
            StructField('order_estimated_delivery_date', DateType(), False)
        ]),
        'order_items': StructType([
            StructField('order_id', StringType(), False),
            StructField('order_item_id', StringType(), False),
            StructField('product_id', StringType(), False),
            StructField('seller_id', StringType(), False),
            StructField('shipping_limit_date', TimestampType(), False),
            StructField('price', FloatType(), False),
            StructField('freight_value', FloatType(), False)
        ]),
        'order_payments': StructType([
            StructField('order_id', StringType(), False),
            StructField('payment_sequential', ByteType(), False),   # for cases of multiple payment methods per order
            StructField('payment_type', StringType(), False),
            StructField('payment_installments', ByteType(), False),
            StructField('payment_value', FloatType(), False)
        ]), 
        'order_reviews': StructType([
            StructField('review_id', StringType(), False),
            StructField('order_id', StringType(), False),
            StructField('review_score', ByteType(), False),   # from 1 to 5
            StructField('review_comment_title', StringType(), True),
            StructField('review_comment_message', StringType(), True),
            StructField('review_creation_date', DateType(), False),
            StructField('review_answer_timestamp', TimestampType(), False)
        ]), 
        'products': StructType([
            StructField('product_id', StringType(), False),
            StructField('product_category_name', StringType(), True),
            StructField('product_name_length', ByteType(), False),
            StructField('product_description_length', ShortType(), True), 
            StructField('product_photos_qty', ByteType(), True),
            StructField('product_weight_g', IntegerType(), False),
            StructField('product_length_cm', ShortType(), False),
            StructField('product_height_cm', ShortType(), False),
            StructField('product_width_cm', ShortType(), False)
        ]), 
        'product_category_name_translation': StructType([
            StructField('product_category_name', StringType(), False),
            StructField('product_category_name_english', StringType(), False)
        ]),
        'sellers': StructType([
            StructField('seller_id', StringType(), False),
            StructField('seller_zip_code_prefix', StringType(), False),
            StructField('seller_city', StringType(), False),
            StructField('seller_state', StringType(), False)
        ])
    }

    def __init__(self, tblname: Literal[AvailableTblnames]):
        if tblname not in get_args(AvailableTblnames):
            raise ValueError('Table name is not available')
        self.tblname = tblname

    def as_StructType(self):
        return self._schemas_StructType[self.tblname]