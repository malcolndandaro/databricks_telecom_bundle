# Databricks notebook source
dbutils.widgets.text("catalog_name","databricks_telecom_bundle", "Nome do catálogo")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# COMMAND ----------

sql_query = f"""
CREATE schema if not exists {catalog_name}.misc
"""
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE VOLUME if not exists {catalog_name}.misc.erbs
COMMENT 'Dados de ERBs, csv e pickle'
"""
spark.sql(sql_query)

# COMMAND ----------

sql_query = f"""
CREATE schema if not exists {catalog_name}.resource_bronze"""
spark.sql(sql_query)

# COMMAND ----------

# current_path = (dbutils.notebook.entry_point
#                 .getDbutils()
#                 .notebook()
#                 .getContext()
#                 .notebookPath()
#                 .get())
# project_folder = "/".join(current_path.split("/")[:-1])

schema = StructType([
    StructField("NumEstacao", LongType(), True),
    StructField("NomeEntidade", StringType(), True),
    StructField("EnderecoEstacao", StringType(), True),
    StructField("SiglaUf", StringType(), True),
    StructField("CodMunicipio", LongType(), True),
    StructField("NomeMunicipio", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longitude", StringType(), True),
    StructField("2G", StringType(), True),
    StructField("3G", StringType(), True),
    StructField("4G", StringType(), True),      
    StructField("5G", StringType(), True),
])

import pandas as pd
df_erb_pd = pd.read_csv(f"file:ERB-Jul24.csv", sep=";")


df_erb = spark.createDataFrame(df_erb_pd, schema=schema)

# COMMAND ----------

display(df_erb) 

# COMMAND ----------

df_erb.write.mode("overwrite").saveAsTable(f"{catalog_name}.resource_bronze.erb_coord")

# COMMAND ----------

erb2 = spark.read.table(f"{catalog_name}.resource_bronze.erb_coord")
