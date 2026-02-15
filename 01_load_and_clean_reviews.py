# Databricks notebook source
storage_account_name = "amazondatalake60304966"
storage_account_key = "REDACTED"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

reviews_path = "abfss://processed@amazondatalake60304966.dfs.core.windows.net/reviews/"

reviews_df = spark.read.parquet(reviews_path)

reviews_df.printSchema()
display(reviews_df.limit(5))


# COMMAND ----------

from pyspark.sql.functions import col, trim, length

clean_reviews_df = reviews_df

# Drop rows with missing critical fields
clean_reviews_df = clean_reviews_df.filter(
    col("asin").isNotNull() &
    col("reviewerID").isNotNull() &
    col("overall").isNotNull()
)

# Enforce valid rating values
clean_reviews_df = clean_reviews_df.filter(
    (col("overall") >= 1) & (col("overall") <= 5)
)

# Clean review text
clean_reviews_df = clean_reviews_df.withColumn(
    "reviewText",
    trim(col("reviewText"))
).filter(
    length(col("reviewText")) >= 10
)


# COMMAND ----------

print("Rows before cleaning:", reviews_df.count())
print("Rows after cleaning:", clean_reviews_df.count())

display(clean_reviews_df.limit(5))


# COMMAND ----------

clean_reviews_path = "abfss://processed@amazondatalake60304966.dfs.core.windows.net/clean_reviews/"

clean_reviews_df.write.mode("overwrite").parquet(clean_reviews_path)
