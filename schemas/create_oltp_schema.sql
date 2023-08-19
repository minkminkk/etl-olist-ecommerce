-- REFRESH SCHEMA IF EXISTS

DROP SCHEMA IF EXISTS oltp_schema CASCADE;

CREATE SCHEMA oltp_schema;


-- CREATE TABLES
CREATE TABLE IF NOT EXISTS oltp_schema.orders (
    order_id                        CHAR(32)    PRIMARY KEY,
    customer_id                     CHAR(32),   -- REFERENCES customers.customer_id
    order_status                    VARCHAR,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS oltp_schema.order_reviews (
    review_id                   CHAR(32)    PRIMARY KEY,
    order_id                    CHAR(32),   -- REFERENCES orders.order_id
    review_score                SMALLINT,   -- From 1 to 5
    review_comment_title        VARCHAR,
    review_comment_message      VARCHAR,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS oltp_schema.order_payments (
    order_id                CHAR(32),   -- REFERENCES orders.order_id
    payment_sequential      VARCHAR,
    payment_type            VARCHAR,
    payment_installments    SMALLINT,
    payment_value           NUMERIC(10,2)   -- Assuming value under 10^8          
);

CREATE TABLE IF NOT EXISTS oltp_schema.customers (
    customer_id                 CHAR(32)    PRIMARY KEY,
    customer_unique_id          CHAR(32),
    customer_zip_code_prefix    INTEGER     -- REFERENCES geolocation
);

CREATE TABLE IF NOT EXISTS oltp_schema.order_items (
    order_id                CHAR(32),   -- REFERENCES orders
    order_item_id           INTEGER,
    product_id              CHAR(32),   -- REFERENCES products,
    seller_id               CHAR(32),   -- REFERENCES sellers,
    shipping_limit_date     TIMESTAMP,
    price                   NUMERIC(10,2),  -- Assuming value under 10^8
    freight_value           NUMERIC(8,2)    -- Assuming value under 10^6
);

CREATE TABLE IF NOT EXISTS oltp_schema.products (
    product_id                      CHAR(32)    PRIMARY KEY,
    product_category_name           VARCHAR,    -- REFERENCES category_name_translation,
    product_name_length             SMALLINT,
    product_description_length      SMALLINT,
    product_photos_qty              SMALLINT,
    product_weight_g                INTEGER,
    product_length_cm               SMALLINT,
    product_height_cm               SMALLINT,
    product_width_cm                SMALLINT
);

CREATE TABLE IF NOT EXISTS oltp_schema.category_name_translation (
    product_category_name           VARCHAR     PRIMARY KEY,
    product_category_name_english   VARCHAR
);

CREATE TABLE IF NOT EXISTS oltp_schema.sellers (
    seller_id                   CHAR(32)    PRIMARY KEY,
    seller_zip_code_prefix      INTEGER     -- REFERENCES geolocation
);

CREATE TABLE IF NOT EXISTS oltp_schema.geolocation (
    geolocation_zip_code_prefix     INTEGER     PRIMARY KEY,
    geolocation_lat                 SMALLINT,
    geolocation_lng                 SMALLINT,
    geolocation_city                VARCHAR,
    geolocation_state               CHAR(2)
);


-- ADD FOREIGN KEY CONSTRAINTS
ALTER TABLE oltp_schema.orders
    ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id)
        REFERENCES oltp_schema.customers (customer_id);

ALTER TABLE oltp_schema.order_reviews
    ADD CONSTRAINT order_reviews_order_id_fkey FOREIGN KEY (order_id)
        REFERENCES oltp_schema.orders (order_id);

ALTER TABLE oltp_schema.order_payments
    ADD CONSTRAINT order_payments_order_id_fkey FOREIGN KEY (order_id)
        REFERENCES oltp_schema.orders (order_id);

ALTER TABLE oltp_schema.customers
    ADD CONSTRAINT customers_customer_zip_code_prefix_fkey FOREIGN KEY (customer_zip_code_prefix)
        REFERENCES oltp_schema.geolocation (geolocation_zip_code_prefix);

ALTER TABLE oltp_schema.order_items
    ADD CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id)
        REFERENCES oltp_schema.orders (order_id);
ALTER TABLE oltp_schema.order_items
    ADD CONSTRAINT order_items_product_id_fkey FOREIGN KEY (product_id)
        REFERENCES oltp_schema.products (product_id);
ALTER TABLE oltp_schema.order_items
    ADD CONSTRAINT order_items_seller_id_fkey FOREIGN KEY (seller_id)
        REFERENCES oltp_schema.sellers (seller_id);

ALTER TABLE oltp_schema.products
    ADD CONSTRAINT products_product_category_name_fkey FOREIGN KEY (product_category_name)
        REFERENCES oltp_schema.category_name_translation (product_category_name);

ALTER TABLE oltp_schema.sellers
    ADD CONSTRAINT sellers_seller_zip_code_prefix_fkey FOREIGN KEY (seller_zip_code_prefix)
        REFERENCES oltp_schema.geolocation (geolocation_zip_code_prefix);