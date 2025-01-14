# Databricks notebook source
dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")


# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create the base feature DataFrame
base_feature_df = spark.sql(f"""
    WITH product AS (
        SELECT nu_tlfn, ds_plno AS product
        FROM customer_silver.product_subscriptions
        WHERE __END_AT IS NULL
    ),
    sva AS (
        SELECT msisdn AS nu_tlfn, productname AS product
        FROM customer_silver.sva_subscriptions 
        WHERE __END_AT IS NULL
    )
    SELECT * FROM product
    UNION ALL
    SELECT * FROM sva
""")

# Get distinct products dynamically
distinct_products = base_feature_df.select("product").distinct().collect()

distinct_products = [x[0] for x in distinct_products]
# Create a list of column expressions for pivot
pivot_exprs = [F.max(F.when(F.col("product") == product, 1).otherwise(0)).alias(f"col{i+1}") 
                for i, product in enumerate(distinct_products)]

# Create the feature store table
feature_store_df = base_feature_df.groupBy("nu_tlfn").agg(*pivot_exprs)

# Show the result
display(feature_store_df)

feature_store_df.write.mode("overwrite").saveAsTable("customer_gold.fs_produtos")
