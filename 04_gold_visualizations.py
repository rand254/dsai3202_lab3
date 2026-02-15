# Databricks notebook source
storage_account_name = "amazondatalake60304966"
storage_account_key = "REDACTED"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

gold_path = "abfss://curated@amazondatalake60304966.dfs.core.windows.net/features_v1/"
gold_df = spark.read.parquet(gold_path)

gold_df.printSchema()
display(gold_df.limit(5))


# COMMAND ----------

ratings_pd = gold_df.select("overall").toPandas()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(8,5))
sns.countplot(data=ratings_pd, x="overall")
plt.title("Distribution of Product Ratings")
plt.xlabel("Rating")
plt.ylabel("Number of Reviews")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC This visualization shows the distribution of product ratings in the Amazon Electronics dataset. The majority of reviews are concentrated around higher ratings (4 and 5 stars), indicating generally positive customer feedback. Lower ratings appear less frequently, suggesting that most products in this dataset receive favorable evaluations.

# COMMAND ----------

from pyspark.sql.functions import avg, count

brand_rating = gold_df.groupBy("brand") \
    .agg(
        avg("overall").alias("avg_rating"),
        count("*").alias("review_count")
    ) \
    .filter("review_count >= 100") \
    .orderBy("avg_rating", ascending=False)

brand_rating_pd = brand_rating.limit(10).toPandas()


# COMMAND ----------

plt.figure(figsize=(10,6))
sns.barplot(data=brand_rating_pd, x="avg_rating", y="brand")

plt.title("Top 10 Brands by Average Rating")
plt.xlabel("Average Rating")
plt.ylabel("Brand")
plt.xlim(0, 5)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC This visualization shows the top 10 brands ranked by their average customer rating. Some brands consistently receive higher ratings, indicating stronger customer satisfaction. This analysis helps identify brands with better perceived product quality in the electronics category.
# MAGIC