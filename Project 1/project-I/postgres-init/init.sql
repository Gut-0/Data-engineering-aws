CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dim_reviews
(
    review_id VARCHAR(255),
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP(0),
    review_answer_timestamp TIMESTAMP(0)
);

CREATE TABLE IF NOT EXISTS dim_payment_types
(
    payment_type_id VARCHAR(255) DEFAULT uuid_generate_v4() PRIMARY KEY,
    payment_type VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_dates
(
    date_id VARCHAR(255),
    order_status VARCHAR(255),
    order_purchase_timestamp TIMESTAMP(0),
    order_approved_at TIMESTAMP(0),
    order_delivered_carrier_date TIMESTAMP(0),
    order_delivered_customer_date TIMESTAMP(0),
    order_estimated_delivery_date TIMESTAMP(0)
);

CREATE TABLE IF NOT EXISTS dim_customers
(
    customer_id VARCHAR(255),
    state_id VARCHAR(255),
    customer_unique_id VARCHAR(255),
    customer_zip_code_prefix VARCHAR(255),
    customer_city VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_products
(
    product_id VARCHAR(255),
    product_category_id VARCHAR(255),
    product_name_length INT,
    product_description_length BIGINT,
    product_photos_qty INT,
    product_weight_g BIGINT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE IF NOT EXISTS dim_states
(
    state_id VARCHAR(255) DEFAULT uuid_generate_v4() PRIMARY KEY,
    state_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_product_categories
(
    product_category_id VARCHAR(255) DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_category_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_sales
(
    sale_id VARCHAR(255) DEFAULT uuid_generate_v4() PRIMARY KEY,
    sale_item_id INT,
    product_category_id VARCHAR(255),
    state_id VARCHAR(255),
    product_id VARCHAR(255),
    customer_id VARCHAR(255),
    date_id VARCHAR(255),
    payment_type_id VARCHAR(255),
    review_id VARCHAR(255),
    review_score INT,
    shipping_limit_date TIMESTAMP(0),
    payment_sequential INT,
    payment_installments INT,
    payment_value FLOAT,
    price FLOAT,
    freight_value FLOAT
);
