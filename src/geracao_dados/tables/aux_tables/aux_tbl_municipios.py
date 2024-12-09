# Databricks notebook source
# MAGIC %md
# MAGIC ## aux_tbl_estados

# COMMAND ----------

dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "misc")
p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0]


# COMMAND ----------

df_estados = spark.read.csv(f"file:/Workspace/{notebook_path}/aux_files/estados.csv", header=True, inferSchema=True)
df_estados.write.mode("overwrite").format("delta").saveAsTable(f"{p_catalog}.{p_schema}.aux_tbl_estados")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $p_catalog.$p_schema.aux_tbl_estados

# COMMAND ----------

# MAGIC %md
# MAGIC ## aux_tbl_municipios

# COMMAND ----------

df_municipios = spark.read.csv(f"file:/Workspace/{notebook_path}/aux_files/municipios.csv", header=True, inferSchema=True)
df_municipios.write.mode("overwrite").format("delta").saveAsTable(f"{p_catalog}.{p_schema}.aux_tbl_municipios")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $p_catalog.$p_schema.aux_tbl_municipios
