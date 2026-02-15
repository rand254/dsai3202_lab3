# Databricks notebook source
storage_account_name = "amazondatalake60304966"
storage_account_key = "REDACTED"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

enriched_reviews_path = "abfss://processed@amazondatalake60304966.dfs.core.windows.net/enriched_reviews/"
enriched_df = spark.read.parquet(enriched_reviews_path)


# COMMAND ----------

features_v1_df = enriched_df.select(
    "asin",
    "title",
    "brand",
    "price",
    "reviewerID",
    "overall",
    "summary",
    "reviewText",
    "helpful",
    "reviewTime",
    "review_year"
)


# COMMAND ----------

gold_path = "abfss://curated@amazondatalake60304966.dfs.core.windows.net/features_v1/"
features_v1_df.write.mode("overwrite").parquet(gold_path)


# COMMAND ----------

gold_df = spark.read.parquet(gold_path)
gold_df.printSchema()
display(gold_df.limit(5))
print("Final row count:", gold_df.count())
