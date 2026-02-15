# Databricks notebook source
storage_account_name = "amazondatalake60304966"
storage_account_key = "REDACTED"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

clean_reviews_path = "abfss://processed@amazondatalake60304966.dfs.core.windows.net/clean_reviews/"
clean_reviews_df = spark.read.parquet(clean_reviews_path)

clean_reviews_df.printSchema()
display(clean_reviews_df.limit(5))


# COMMAND ----------

metadata_path = "abfss://raw@amazondatalake60304966.dfs.core.windows.net/meta_Electronics_fixed.json"
metadata_df = spark.read.json(metadata_path)

metadata_df.printSchema()
display(metadata_df.limit(5))


# COMMAND ----------

metadata_df = metadata_df.select(
    "asin",
    "title",
    "brand",
    "price"
)


# COMMAND ----------

enriched_df = clean_reviews_df.join(
    metadata_df,
    on="asin",
    how="left"
)

display(enriched_df.limit(5))


# COMMAND ----------

enriched_reviews_path = "abfss://processed@amazondatalake60304966.dfs.core.windows.net/enriched_reviews/"

enriched_df.write.mode("overwrite").parquet(enriched_reviews_path)
