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

import pandas as pd

# Read CSV using pandas
df_estados_pd = pd.read_csv(f"/Workspace/{notebook_path}/aux_files/estados.csv")

# Convert pandas DataFrame to Spark DataFrame
df_estados = spark.createDataFrame(df_estados_pd)

# Write Spark DataFrame to Delta table
df_estados.write.mode("overwrite").format("delta").saveAsTable(f"{p_catalog}.{p_schema}.aux_tbl_estados")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $p_catalog.$p_schema.aux_tbl_estados

# COMMAND ----------

# MAGIC %md
# MAGIC ## aux_tbl_municipios

# COMMAND ----------

# Read CSV using pandas
df_municipios_pd = pd.read_csv(f"/Workspace/{notebook_path}/aux_files/municipios.csv")

# Convert pandas DataFrame to Spark DataFrame
df_municipios = spark.createDataFrame(df_municipios_pd)

# Write Spark DataFrame to Delta table
df_municipios.write.mode("overwrite").format("delta").saveAsTable(f"{p_catalog}.{p_schema}.aux_tbl_municipios")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $p_catalog.$p_schema.aux_tbl_municipios
