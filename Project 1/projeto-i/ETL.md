# Projeto-i

- [Description](#description) 
- [Data Extraction](#data-extraction)
    - [Extracting Data from MongoDB](#extracting-data-from-mongodb) 
    - [Extracting Data from CSV Files](#extracting-data-from-csv-files) 
- [Dimension Tables](#dimension-tables) 
    - [Reviews Dimension Table](#reviews-dimension-table-dimreviews)
    - [States Dimension Table](#states-dimension-table-dimstates)
    - [Customers Dimension Table](#customers-dimension-table-dimcustomers)
    - [Product Categories Dimension Table](#product-categories-dimension-table-dimproductcategories)
    - [Products Dimension Table](#products-dimension-table-dimproducts)
    - [Payment Types Dimension Table](#payment-types-dimension-table-dimpaymenttypes)
    - [Dates Dimension Table](#dates-dimension-table-dimdates)
- [Fact Sales Table](#fact-sales-table-factsales)
- [Main ETL Workflow](#main-etl-workflow)

## Description

This project implements a small-scale Data Warehouse using Python for ETL (Extract, Transform, Load) processes. The primary goal is to extract data from multiple sources, transform it into a suitable format, and load it into a PostgreSQL database following a dimensional modeling approach. The project consists of several components and functions designed to achieve this goal efficiently.

## Data Extraction

#### Extracting Data from MongoDB
* **Function**: extract_data_from_mongodb()
* **Description**: This function connects to MongoDB 'ecommerce' database and extracts data from 'order_reviews' collection. The extracted data is then converted into a Pandas DataFrame. The extraction process is facilitated by the PyMongo library, and the resulting DataFrame provides a structured representation of MongoDB data for further manipulation.

#### Extracting Data from CSV Files
* **Description**: Data from CSV files is loaded into Pandas DataFrames, which serve as the foundation for populating dimension and fact tables. The CSV files contain essential information required for the ETL process.


## Dimension Tables

#### Reviews Dimension Table (dim_reviews)
* **Function**: populate_dim_reviews()
* **Description**: This function is responsible for populating the dim_reviews dimension table. It first removes unnecessary columns from the reviews DataFrame and handles missing values by mapping them to None. The resulting data is then inserted into the PostgreSQL database.

#### States Dimension Table (dim_states)
* **Function**: populate_dim_states()
* **Description**: The dim_states dimension table stores unique customer state data. This function extracts unique state values from the customer data DataFrame and inserts them into the table.

#### Customers Dimension Table (dim_customers)
* **Function**: populate_dim_customers()
* **Description**: Customer data is merged with state data from the dim_states table. The merged data is then inserted into the dim_customers dimension table. Care is taken to handle missing values effectively during the process.

#### Product Categories Dimension Table (dim_product_categories)
* **Function**: populate_dim_product_categories()
* **Description**: Unique product category data is extracted from the product categories DataFrame and stored in the dim_product_categories dimension table.

#### Products Dimension Table (dim_products)
* **Function**: populate_dim_products()
* **Description**: Product data is merged with product category information from the dim_product_categories table. The combined data is then inserted into the dim_products dimension table. Column names are standardized during the process.

#### Payment Types Dimension Table (dim_payment_types)
* **Function**: populate_dim_payment_types()
* **Description**: Unique payment type data is extracted from the payment types DataFrame and stored in the dim_payment_types dimension table.

#### Dates Dimension Table (dim_dates)
* **Function**: populate_dim_dates()
* **Description**: Date-related data, such as order dates, is extracted from the dates. NaN values are mapped to None, and the data is inserted into the dim_dates dimension table.

## Fact Sales Table (fact_sales)
* **Function**: populate_fact_sales()
* **Description**: The ETL process prepares the sales data by utilizing the preprocess_fact_sales_data() function. This function cleans the data by removing unnecessary columns and handling missing values. The cleaned data is then merged with related data from dimension tables (dim_customers, dim_products, dim_payment_types, and dim_dates) and inserted into the fact_sales fact table. Renaming of columns is performed to ensure compatibility with the target schema.

## Main ETL Workflow
* **Function**: main()
* **Description**: The main() function serves as the entry point for the ETL process. It orchestrates the entire workflow, including database connection setup, data extraction from MongoDB and CSV files, population of dimension tables, preprocessing and loading of sales data into the fact table.