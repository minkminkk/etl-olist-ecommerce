import pyspark.sql


def get_tblname_from_csvname(csvname:str) -> str:
    """
    Get the table name for the respective csv filename in olist-data.zip

    Argument:
    - csvname: Name of csv file

    Returns:
    - The respective table name if csv filename exists
    - None otherwise
    """

    CSVNAME_TBLNAME_MAP = {
        'olist_geolocation_dataset.csv': 'geolocation',
        'olist_sellers_dataset.csv': 'sellers',
        'olist_customers_dataset.csv': 'customers',
        'product_category_name_translation.csv': 'product_category_name_translation',
        'olist_products_dataset.csv': 'products',
        'olist_orders_dataset.csv': 'orders',
        'olist_order_reviews_dataset.csv': 'order_reviews',
        'olist_order_payments_dataset.csv': 'order_payments',
        'olist_order_items_dataset.csv': 'order_items'
    }

    return CSVNAME_TBLNAME_MAP.get(csvname, None)    