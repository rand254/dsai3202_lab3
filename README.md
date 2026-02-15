This lab demonstrates an end-to-end data preprocessing pipeline built using Azure Databricks and Azure Data Lake Storage Gen2.

The goal of this lab was to implement a Medallion Architecture pipeline (Bronze, Silver, Gold) using Spark in a cloud environment.

In the Bronze layer, raw Amazon Electronics reviews and product metadata were stored in Azure Data Lake Storage Gen2. These datasets were accessed from Databricks using storage account credentials and the ABFSS protocol.

In the Silver layer, the reviews dataset was cleaned using PySpark. Null values were removed, unnecessary fields were dropped, and a new column called review_year was extracted from the review timestamp. Then, the cleaned reviews were enriched by joining them with the metadata dataset (asin, title, brand, price) using a left join in Spark.

In the Gold layer, a curated dataset was created for analytics purposes and saved in parquet format to the curated container. Parquet was used because it is a columnar storage format optimized for big data processing in Spark.

The implementation was divided into separate notebooks:

01_load_and_clean_reviews: loads raw data and performs cleaning operations using Spark transformations.

02_enrich_with_metadata: selects relevant metadata columns and joins them with the reviews dataset.

03_write_gold_features_v1: writes the final curated dataset to the Gold layer.

04_gold_visualizations: performs analysis and creates visualizations using Matplotlib and Seaborn.

A Databricks Job was created to automate the pipeline execution. A schedule was configured to demonstrate production readiness, and then paused to avoid unnecessary cloud costs.

The following technologies were used:

Azure Databricks for distributed data processing

Apache Spark (PySpark) for large-scale transformations

Azure Data Lake Storage Gen2 for cloud storage

Parquet format for efficient storage

Matplotlib and Seaborn for visualization

This lab demonstrates how to build, automate, and analyze a scalable cloud-based data engineering workflow.
