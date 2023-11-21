import pandas as pd
import pymongo
import sqlalchemy

from config.base import settings
from postgres_db import create_postgresql_connection


def extract_data_from_mongodb() -> pd.DataFrame or None:
    """
    Extracts data from MongoDB and converts it into a pandas DataFrame.

    :return: A DataFrame containing the extracted data if successful,
        or None if an error occurs during the extraction.
    """
    try:
        mongo_client = pymongo.MongoClient(settings.mongodb.get("client"))
        mongo_db = mongo_client[settings.mongodb.get("dbname")]
        order_reviews_collection = mongo_db[settings.mongodb.get("collection")]

        order_reviews_data = list(order_reviews_collection.find())
        df = pd.DataFrame(order_reviews_data)

        return df
    except Exception as e:
        print("Erro ao extrair dados do MongoDB:", e)
        return None


def populate_dim_reviews(engine: sqlalchemy.Engine, reviews_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing reviews data and inserts into the 'dim_reviews' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param reviews_df: (pd.core.frame.DataFrame): DataFrame containing reviews data.
    :return: None
    """
    try:
        df = reviews_df
        table_name = 'dim_reviews'

        df = df.drop(['order_id', '_id'], axis=1)
        df = df.map(lambda x: None if pd.isna(x) else x)

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_reviews' table:", e)


def populate_dim_states(engine: sqlalchemy.Engine, customer_states_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing customer states data, extracts unique states,
    and inserts them into the 'dim_states' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param customer_states_df: (pd.core.frame.DataFrame): DataFrame containing reviews data.
    :return: None
    """
    try:
        table_name = 'dim_states'

        unique_states = customer_states_df['customer_state'].unique()
        unique_states_df = pd.DataFrame({'state_name': unique_states})

        unique_states_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_states' table:", e)


def populate_dim_customers(engine: sqlalchemy.Engine, customers_df: pd.DataFrame) -> None:
    """
    Populates the 'dim_customers' table in a database with customer data from a DataFrame.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param customers_df: (pd.core.frame.DataFrame): DataFrame containing customer data.
    :return: None
    """
    try:
        query = "SELECT * FROM dim_states;"
        table_name = 'dim_customers'

        states_df = pd.read_sql(query, engine)
        customers_df = customers_df.map(lambda x: None if pd.isna(x) else x)

        merged_df = customers_df.merge(states_df, left_on='customer_state', right_on='state_name', how='left')
        merged_df.drop(columns=['customer_state', 'state_name'], inplace=True)

        merged_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_customers' table:", e)


def populate_dim_product_categories(engine: sqlalchemy.Engine, categories_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing products categories data, extracts unique categories,
    and inserts them into the 'dim_product_categories' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param categories_df: (pd.core.frame.DataFrame): The data to be inserted.
    :return: None
    """
    try:
        table_name = 'dim_product_categories'

        unique_categories = categories_df['product_category_name'].unique()
        unique_categories_df = pd.DataFrame({'product_category_name': unique_categories}).dropna().drop_duplicates()

        unique_categories_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'product_categories' table:", e)


def populate_dim_products(engine: sqlalchemy.Engine, products_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing product dat, merges it with product categories,
    and inserts the combined data into the 'dim_products' table in the database.
    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param products_df: (pd.core.frame.DataFrame): DataFrame containing product data.
    :return: None
    """
    try:
        query = "SELECT * FROM dim_product_categories;"
        table_name = 'dim_products'

        product_category_ids_df = pd.read_sql(query, engine)
        products_df = products_df.map(lambda x: None if pd.isna(x) else x)

        merged_df = products_df.merge(product_category_ids_df, on='product_category_name', how='left')
        merged_df = merged_df.drop(columns=['product_category_name'])

        merged_df.rename(columns={
            'product_name_lenght': 'product_name_length',
            'product_description_lenght': 'product_description_length'
        }, inplace=True)

        merged_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_products' table:", e)


def populate_dim_payment_types(engine: sqlalchemy.Engine, payment_type_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing payment type data, extracts unique payment types,
    and inserts them into the 'dim_payment_types' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param payment_type_df: (pd.core.frame.DataFrame): A DataFrame containing payment type data.
    :return: None
    """
    try:
        table_name = 'dim_payment_types'

        unique_payment_types = payment_type_df['payment_type'].unique()
        unique_payment_types_df = pd.DataFrame({'payment_type': unique_payment_types})

        unique_payment_types_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_payment_types' table:", e)


def populate_dim_dates(engine: sqlalchemy.Engine, dates_df: pd.DataFrame) -> None:
    """
    This function takes a DataFrame containing date-related data,
    maps NaN values to None and inserts the data into the 'dim_dates' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param dates_df: (pd.core.frame.DataFrame): A DataFrame containing the data to be inserted into the table.
    :return: None
    """
    try:
        df = dates_df.drop('customer_id', axis=1)
        table_name = 'dim_dates'

        df = df.map(lambda x: None if pd.isna(x) else x)
        df = df.rename(columns={'order_id': 'date_id'})

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'dim_dates' table:", e)


def populate_fact_sales(engine: sqlalchemy.Engine, merged_df: pd.DataFrame) -> None:
    """
    This function uses 'preprocess_fact_sales_data()' function to prepare the DataFrame,
    merges it with data from related tables, and inserts it into the 'fact_sales' table.

    :param engine: (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    :param merged_df: (pd.core.frame.DataFrame): A DataFrame containing the data to be inserted into the table.
    :return: None
    """
    try:
        df = preprocess_fact_sales_data(merged_df)
        table_name = 'fact_sales'

        query = "SELECT customer_id, state_id FROM public.dim_customers"
        states_df = pd.read_sql(query, engine)

        query = "SELECT * FROM public.dim_payment_types"
        payment_types = pd.read_sql(query, engine)

        query = "SELECT product_id, product_category_id FROM public.dim_products"
        products_df = pd.read_sql(query, engine)

        df = df.merge(states_df, on='customer_id', how='inner')
        df = df.merge(products_df, on='product_id', how='left')
        df = df.merge(payment_types, on='payment_type', how='left')

        df.drop(columns=['payment_type'], inplace=True)

        df.rename(columns={
            'order_id': 'date_id',
            'order_item_id': 'sale_item_id'
        }, inplace=True)

        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error loading data into the 'fact_sales' table:", e)


def preprocess_fact_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the input DataFrame by dropping unnecessary columns and replacing NaN values with None.

    :param df: (pd.core.frame.DataFrame): The input DataFrame.
    :return: pd.core.frame.DataFrame: The preprocessed DataFrame.
    """
    columns_to_drop = [
        'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
        'order_delivered_customer_date', 'order_estimated_delivery_date', 'seller_id', '_id',
        'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'
    ]

    df = df.drop(columns=columns_to_drop)
    df = df.map(lambda x: None if pd.isna(x) else x)

    return df


def main():
    engine = create_postgresql_connection()
    if engine is None:
        return

    reviews_df = extract_data_from_mongodb()
    if reviews_df is None:
        return

    df = pd.read_csv('input/olist_customers_dataset.csv', dtype={
        'customer_id': str,
        'customer_unique_id': str,
        'customer_zip_code_prefix': str,
        'customer_city': str,
        'customer_state': str
    })
    populate_dim_states(engine, df)
    populate_dim_customers(engine, df)

    df = pd.read_csv('input/olist_products_dataset.csv')
    populate_dim_product_categories(engine, df)
    populate_dim_products(engine, df)
    del df

    order_payments_df = pd.read_csv('input/olist_order_payments_dataset.csv', dtype={
        'order_id': str,
        'payment_sequential': int,
        'payment_type': str,
        'payment_installments': int,
        'payment_value': float
    })
    order_items_df = pd.read_csv('input/olist_order_items_dataset.csv', dtype={
        'order_id': str,
        'order_item_id': int,
        'product_id': str,
        'seller_id': str,
        'price': float,
        'freight_value': float
    }, parse_dates=['shipping_limit_date'])
    orders_df = pd.read_csv('input/olist_orders_dataset.csv', dtype={
        'order_id': str,
        'customer_id': str,
        'order_status': str,
    }, parse_dates=[
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ])

    merged_df = order_items_df.merge(orders_df, on='order_id', how='outer')
    merged_df = merged_df.merge(order_payments_df, on='order_id', how='outer')
    merged_df = merged_df.merge(reviews_df, on='order_id', how='outer')
    del order_items_df

    populate_dim_reviews(engine, reviews_df)
    del reviews_df

    populate_dim_payment_types(engine, order_payments_df)
    del order_payments_df

    populate_dim_dates(engine, orders_df)
    del orders_df

    populate_fact_sales(engine, merged_df)
    del merged_df


if __name__ == "__main__":
    main()
