from typing import Union


# Tables for ingestion
def get_available_tbl_names():
    TBL_NAMES = [
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
    return TBL_NAMES


def get_csvname_from_tblname(tbl_name: str) -> Union[str, None]:
    """
    Get the table name for the respective csv file name in olist-data.zip

    Argument:
    - csvname: Name of csv file

    Returns:
    - The respective table name if csv_name exists
    - None otherwise
    """
    TBLNAME_CSVNAME_MAP = {
        'geolocation': 'olist_geolocation_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'customers': 'olist_customers_dataset.csv',
        'product_category_name_translation': 'product_category_name_translation.csv',
        'products': 'olist_products_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'order_reviews': 'olist_order_reviews_dataset.csv',
        'order_payments': 'olist_order_payments_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv'
    }

    return TBLNAME_CSVNAME_MAP.get(tbl_name, None)    