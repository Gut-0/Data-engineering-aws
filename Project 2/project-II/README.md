# Project II - Data Lake with AWS

This project focuses on building a data lake on AWS, ingesting data from various sources, and performing analysis using AWS services.

## Project Overview

This project involved building a data lake on AWS, ingesting movie data from CSV files and the TMDB API, processing it using AWS Glue, and visualizing the results with AWS QuickSight. The data lake follows a layered architecture (Raw, Trusted, Refined) and utilizes various AWS services like S3, Lambda, Glue, Athena, and QuickSight.

## Project Architecture

The project architecture consists of the following components:

- **Data Sources:** CSV files and TMDB API.
- **Data Ingestion:** 
    - Batch ingestion of CSV files to S3 using Pandas.
    - Real-time ingestion of data from TMDB API to S3 using AWS Lambda.
- **Data Storage:** AWS S3 for storing data in different layers (Raw, Trusted, Refined).
- **Data Processing:** AWS Glue for ETL (Extract, Transform, Load) processes.
- **Data Catalog:** AWS Glue Data Catalog for metadata management.
- **Query Engine:** AWS Athena for querying data in S3.
- **Data Visualization:** AWS QuickSight for creating interactive dashboards.

## Project Workflow

1. **Data Ingestion:** Data is ingested from CSV files and the TMDB API and stored in the Raw layer of the S3 data lake.
2. **Data Transformation:** AWS Glue jobs process the raw data, transform it into a structured format (Parquet), and store it in the Trusted layer.
3. **Data Refinement:** Further Glue jobs refine the data, creating dimension and fact tables, and store it in the Refined layer.
4. **Data Cataloging:** AWS Glue Crawlers create metadata for the data in the Glue Data Catalog, making it available for querying with Athena.
5. **Data Analysis and Visualization:** AWS Athena is used to query the data, and AWS QuickSight is used to create interactive dashboards for analysis and visualization.

## Key Features

- **Layered Data Lake Architecture:** Implementation of a Raw, Trusted, and Refined data lake architecture for data organization and processing.
- **AWS Lambda for Real-time Ingestion:** Leveraging AWS Lambda for serverless, event-driven data ingestion from the TMDB API.
- **AWS Glue for ETL:** Utilizing AWS Glue for serverless ETL processing, transforming data into Parquet format.
- **Data Partitioning:** Partitioning data in S3 for efficient querying and cost optimization.
- **Interactive Dashboards:** Creating insightful dashboards in AWS QuickSight to visualize movie genre trends and ratings.

## Dashboard and Analysis

The project includes an interactive dashboard in AWS QuickSight to analyze movie genre trends and ratings. The dashboard explores questions like:

- What are the highest-rated movie genres?
- How do genre ratings vary over time?
- What is the relationship between average ratings and vote counts?

## Challenges and Learnings

This project provided valuable experience in:

- **AWS Cloud Services:** Utilizing various AWS services like S3, Lambda, Glue, Athena, and QuickSight.
- **Data Lake Implementation:** Designing and implementing a layered data lake architecture on AWS.
- **Serverless Computing:** Leveraging AWS Lambda for serverless data ingestion.
- **ETL Processing:** Developing ETL pipelines using AWS Glue.
- **Data Visualization and Analysis:** Creating interactive dashboards and performing analysis with AWS QuickSight.
