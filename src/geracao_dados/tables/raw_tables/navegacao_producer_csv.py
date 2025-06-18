# Databricks notebook source
# MAGIC %md
# MAGIC # Navegação

# COMMAND ----------

dbutils.widgets.text("p_catalog", "dev")

# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")

# COMMAND ----------

p_schema_resource_bronze = dbutils.widgets.get("p_schema_resource_bronze")
p_schema_misc = dbutils.widgets.get("p_schema_misc")
table_name = f"{p_catalog}.{p_schema_resource_bronze}.navegacao_test_csv"
source_table = f"{p_catalog}.{p_schema_misc}.tbl_navegacao"
data_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/telecom/navegacao/"
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
