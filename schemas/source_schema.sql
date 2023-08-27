DROP DATABASE IF EXISTS olist;
CREATE SCHEMA olist;

/* CREATE TABLES */
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix     CHAR(5)     PRIMARY KEY,
    geolocation_lat                 DOUBLE PRECISION,
    geolocation_lng                 DOUBLE PRECISION,
    geolocation_city                VARCHAR,
    geolocation_state               CHAR(2)
);
CREATE TABLE IF NOT EXISTS sellers (
    seller_id                   CHAR(32)    PRIMARY KEY,
    seller_zip_code_prefix      CHAR(5)     REFERENCES geolocation (geolocation_zip_code_prefix)
);
CREATE TABLE IF NOT EXISTS customers (
    customer_id                 CHAR(32)    PRIMARY KEY,
    customer_unique_id          CHAR(32),
    customer_zip_code_prefix    CHAR(5)     REFERENCES geolocation (geolocation_zip_code_prefix)
);
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name           VARCHAR     PRIMARY KEY,
    product_category_name_english   VARCHAR
);
CREATE TABLE IF NOT EXISTS products (
    product_id                      CHAR(32)    PRIMARY KEY,
    product_category_name           VARCHAR     REFERENCES product_category_name_translation (product_category_name),
    product_name_length             SMALLINT,
    product_description_length      SMALLINT,
    product_photos_qty              SMALLINT,
    product_weight_g                INTEGER,
    product_length_cm               SMALLINT,
    product_height_cm               SMALLINT,
    product_width_cm                SMALLINT
);
CREATE TABLE IF NOT EXISTS orders (
    order_id CHAR(32) PRIMARY KEY,
    customer_id CHAR(32)     REFERENCES customers (customer_id),
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id                   CHAR(32)    PRIMARY KEY,
    order_id                    CHAR(32)    REFERENCES orders (order_id),
    review_score                SMALLINT,   -- From 1 to 5
    review_comment_title        VARCHAR,
    review_comment_message      VARCHAR,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP
);
CREATE TABLE IF NOT EXISTS order_payments (
    order_id                CHAR(32)        REFERENCES orders (order_id),
    payment_sequential      VARCHAR,
    payment_type            VARCHAR,
    payment_installments    SMALLINT,
    payment_value           NUMERIC(10,2)   -- Assuming value under 10^8          
);
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id           SERIAL,
    order_id                CHAR(32)    REFERENCES orders (order_id),
    order_item_no           SMALLINT,
    product_id              CHAR(32)    REFERENCES products (product_id),
    seller_id               CHAR(32)    REFERENCES sellers (seller_id),
    shipping_limit_date     TIMESTAMP,
    price                   NUMERIC(10,2),  -- Assuming value under 10^8
    freight_value           NUMERIC(8,2)    -- Assuming value under 10^6
);
