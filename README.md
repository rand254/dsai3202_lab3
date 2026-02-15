This lab demonstrates an end-to-end data preprocessing pipeline built using Azure Databricks and Azure Data Lake Storage Gen2.

In this project, I implemented the Medallion Architecture approach:

Bronze layer: Raw Amazon Electronics reviews and metadata were stored in Azure Data Lake Storage Gen2.
Silver layer: The reviews dataset was cleaned by removing null values and extracting useful fields such as review_year. Then, it was enriched by joining it with the product metadata dataset (asin, title, brand, price).
Gold layer: A curated dataset was created for analytics purposes and saved to the curated layer in ADLS Gen2.

I created separate notebooks for each step:

01_load_and_clean_reviews: loads raw reviews and performs data cleaning.

02_enrich_with_metadata: joins cleaned reviews with product metadata.

03_write_gold_features_v1: writes the curated Gold dataset.

04_gold_visualizations: performs data analysis and creates visualizations.

I also created a Databricks Job to automate the pipeline and configured a schedule (then paused it to avoid unnecessary costs).

For analysis, I generated:

A rating distribution visualization.

A top 10 brands by average rating visualization.

Technologies used in this lab include Azure Databricks, PySpark, Azure Data Lake Storage Gen2, Matplotlib, and Seaborn.

This lab demonstrates how to build, automate, and analyze a scalable cloud-based data processing pipeline.
