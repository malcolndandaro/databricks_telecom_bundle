# Databricks notebook source
# MAGIC %md
# MAGIC # Navegação

# COMMAND ----------

dbutils.widgets.text("p_catalog", "dev")

# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")

# COMMAND ----------

table_name = f"{p_catalog}.resource_bronze.navegacao_test_csv"
source_table = f"{p_catalog}.misc.tbl_navegacao"
data_path = f"/Volumes/{p_catalog}/ingestion/raw_data/telecom/navegacao/"
#dbutils.fs.rm(data_path, recurse=True)
spark.sql(f"drop table if exists {table_name}")

# COMMAND ----------

df_navegacao = (
    spark
    .read
    .table(f"{source_table}")
)

# COMMAND ----------

(
    df_navegacao.write.format("csv")
    .option("compression", "snappy")
    .option("path", data_path)
    .mode("overwrite")
    .save()
)
