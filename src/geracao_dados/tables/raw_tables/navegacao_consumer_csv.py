# Databricks notebook source
# MAGIC %md
# MAGIC # Navegação
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema_table", "resource_bronze.navegacao_test_csv")
dbutils.widgets.text("p_schema_resource_bronze", "resource_bronze")

# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")
p_schema_table = dbutils.widgets.get("p_schema_table")
p_schema_resource_bronze = dbutils.widgets.get("p_schema_resource_bronze")

# COMMAND ----------

checkpoint_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/checkpoints/telecom/navegacao/"
table_name = f"{p_catalog}.{p_schema_table}"
data_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/telecom/navegacao/"
dbutils.fs.rm(checkpoint_path, recurse=True)
if table_name.split(".")[2] == 'navegacao_test_csv':
    spark.sql(f"drop table if exists {table_name}")


# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DateType
)

schema = StructType([
    StructField('nr_tlfn', StringType(), True),
    StructField('ds_ip', StringType(), True),
    StructField('cd_imei', StringType(), True),
    StructField('cd_cgi', IntegerType(), True),
    StructField('dt_ini_nvgc', LongType(), True),
    StructField('dt_fim_nvgc', LongType(), True),
    StructField('ds_host', StringType(), True),
    StructField('ds_pctl', StringType(), True),
    StructField('ds_sbpt', StringType(), True),
    StructField('qtdd_byte_tfgd', IntegerType(), True),
    StructField('partition_date', DateType(), True)
])

# COMMAND ----------

total_csv_size_in_gb = sum([x.size for x in dbutils.fs.ls(data_path)])/( 1024*1024*1024)
print(f"Total size of all CSV files in GB: {total_csv_size_in_gb}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        select count(*) from {p_catalog}.resource_bronze.tbl_navegacao
        where partition_date in ('2024-10-01')
"""
    )
)

# COMMAND ----------

import time
start_time = time.time()

load_from_csv = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.maxFilesPerTrigger", sc.defaultParallelism)
    .schema(schema)
    .load(data_path)
    .writeStream 
    .queryName("load_from_csv") 
    .format("delta") 
    .option("checkpointLocation", checkpoint_path) 
    .trigger(availableNow=True)
    .toTable(table_name)
    
)


# COMMAND ----------

load_from_csv.awaitTermination()

# COMMAND ----------

end_time = time.time()
execution_time = round(end_time - start_time)
print(execution_time)

# COMMAND ----------

display(
    spark.sql(
        f"""
        select count(*) from {p_catalog}.resource_bronze.tbl_navegacao
        where partition_date in ('2024-10-01')
"""
    )
)

# COMMAND ----------

display(
    spark.sql(f"""
              select count(*) as total_rows, 
              sum(octet_length(concat(*)))/(1024*1024) as total_mb,
              {execution_time} as execution_time_seconds,
              total_mb/execution_time_seconds as mb_per_second,
              (mb_per_second*60)/1024 as gb_per_minute
              from {table_name}
              where partition_date in ('2024-10-01')
              """)
)
