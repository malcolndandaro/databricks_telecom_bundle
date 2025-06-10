# Databricks notebook source
import dlt
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    ShortType,
    ByteType,
    DecimalType,
)
from pyspark.sql.functions import current_timestamp, expr

# COMMAND ----------

try: 
    spark.conf.get("confs.p_catalog")
except:
    spark.conf.set("confs.p_catalog", "dev")
p_catalog = spark.conf.get("confs.p_catalog")
data_path = f"/Volumes/{p_catalog}/ingestion/raw_data/billing/invoicing/"

# COMMAND ----------


@dlt.table()
def invoicing(cluster_by="dt_ciclo_rcrg"):
    return (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.partitionColumns", "")
    .load(data_path)
    .withColumn("bronze_ts", current_timestamp())
    .withColumn("_metadata_file_path", expr("_metadata.file_path"))
    .withColumn("_metadata_file_modification_time", expr("_metadata.file_modification_time"))
 )
    


# COMMAND ----------

from pyspark.sql.functions import count

@dlt.view()
@dlt.expect("nu_doct_dt_ciclo_rcrg", "count = 1")
def check_uniquiness():
    df_invoicing = spark.read.table("LIVE.invoicing")
    df_uniquiness = (
    df_invoicing
    .groupBy(["nu_doct", "dt_ciclo_rcrg"])
    .agg(count("*").alias("count"))
)
    return df_uniquiness

# COMMAND ----------

@dlt.table()
def invoicing_quarantine():
    df_invoicing = spark.read.table("LIVE.invoicing")
    df_uniquiness = spark.read.table("LIVE.check_uniquiness").filter("count >= 2")
    df_quarantine = df_invoicing.join(df_uniquiness, ["nu_doct", "dt_ciclo_rcrg"], "inner").select(df_invoicing.columns + ["count"])
    return df_quarantine
