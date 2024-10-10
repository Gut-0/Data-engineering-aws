# Project I - Data Engineering with AWS

This project focuses on extracting data from MongoDB, transforming it, and loading it into a PostgreSQL database, all within a Docker environment.

## Project Overview

This project involved building an ETL (Extract, Transform, Load) pipeline to process data from a MongoDB database and load it into a PostgreSQL data warehouse. The pipeline is built using Python and utilizes various libraries such as Pandas, SQLAlchemy, and Psycopg2.

## Project Structure

The project is organized as follows:

- **database_model:** Contains the data model diagram for the PostgreSQL data warehouse.
- **environments:**  Holds environment variables for the PostgreSQL database.
- **postgres-init:** Contains SQL scripts to initialize the PostgreSQL database schema.
- **python_etl:**  Contains the Python code for the ETL pipeline.
    - **source:**  Source code for the ETL process.
        - **config:** Configuration files for database connections.

## Challenges and Solutions

During this project, I encountered several challenges and developed solutions to overcome them:

- **CSV Data Handling:** Addressed issues with data cleaning, null values, and inconsistent data types in the CSV files.
- **Performance Optimization:** Improved ETL performance by refactoring code, utilizing Pandas `to_sql` method for bulk inserts, and optimizing data manipulation techniques.
- **Docker Implementation:** Learned to build and manage a Docker environment, including creating Dockerfiles and docker-compose configurations.
- **Data Modeling:** Designed and refined a dimensional data model for the data warehouse.

## Key Learnings

This project provided valuable experience in:

- **Data Engineering:** Implementing an ETL pipeline, data cleaning, and transformation.
- **Python Programming:** Utilizing Pandas for data manipulation and SQLAlchemy for database interaction.
- **SQL:** Designing a data warehouse schema and writing SQL queries.
- **Docker:** Building and managing a containerized environment for development and deployment.
- **Data Modeling:** Designing a dimensional model for a data warehouse.
