# Databricks notebook source
# MAGIC %pip install faiss-cpu==1.8.0.post1
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %md
# MAGIC # Introdução
# MAGIC
# MAGIC Este notebook gera um conjunto de dados para servir como base para a Geracao de dados com geolocalização.
# MAGIC A tabela `aux_enderecos` foi criada usando um conjunto de dados real de ERBs. Este conjunto de dados contém geolocalizações das ERBs, que foram usadas como pontos de referência para criar endereços falsos nas proximidades.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col, lit, unix_timestamp, expr, to_timestamp, array, explode, rand
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd
import numpy as np
from datetime import datetime
import faiss
import pickle

# COMMAND ----------

dbutils.widgets.text("begin_time_home", "0","Begin Time at home")
dbutils.widgets.text("end_time_home", "6","End Time at home")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Reference Date")
dbutils.widgets.text("qt_GB_month", "25","GB p/ Month")
dbutils.widgets.text("catalog_name","databricks_telecom_bundle", "Nome do catálogo")
dbutils.widgets.text("amostra","False", "Trabalhar com Amostra?")


data_ref            = dbutils.widgets.get("data_ref")
begin_time_home     = int(dbutils.widgets.get("begin_time_home"))
end_time_home       = int(dbutils.widgets.get("end_time_home"))
qt_GB_month         = int(dbutils.widgets.get("qt_GB_month"))
catalog_name        = dbutils.widgets.get("catalog_name")

amostra             = dbutils.widgets.get("amostra") == "True"


# COMMAND ----------

from pyspark.sql.functions import rand, col

p_schema_misc = dbutils.widgets.get("p_schema_misc")
clients = spark.read.table(f"{catalog_name}.{p_schema_misc}.aux_tbl_clientes")

clients = (
    clients.withColumn("id_addr", (rand() * 1436920).cast("int")) # 1436920 is the number of addresses in the table
     .select("id_addr", "nu_tlfn", "nu_imei_aprl", "cd_ddd")
)

if (amostra):
    clients = clients.sample(False, fraction=1.0).limit(10000)

address = spark.read.table(f"{catalog_name}.{p_schema_misc}.aux_enderecos")
client_location = ( 
    clients
    .join(address, on = (address.id == clients.id_addr))
)

client_location1 = client_location.select("nu_tlfn",
                                         col("erb_preferida").alias('cd_cgi'), 
                                         "nu_imei_aprl", 
                                         col("cd_ddd").alias("cd_area"),                                         
                                         col("lon").alias("lon_res"), 
                                         col("lat").alias("lat_res"))

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import ArrayType, IntegerType

# def get_area_code(state):
#     area_codes = {
#         "AC": [68], "AL": [82], "AP": [96], "AM": [92, 97], "BA": [71, 73, 74, 75, 77],
#         "CE": [85, 88], "DF": [61], "ES": [27, 28], "GO": [62, 64], "MA": [98, 99],
#         "MT": [65, 66], "MS": [67], "MG": [31, 32, 33, 34, 35, 37, 38], "PA": [91, 93, 94],
#         "PB": [83], "PR": [41, 42, 43, 44, 45, 46], "PE": [81, 87], "PI": [86, 89],
#         "RJ": [21, 22, 24], "RN": [84], "RS": [51, 53, 54, 55], "RO": [69], "RR": [95],
#         "SC": [47, 48, 49], "SP": [11, 12, 13, 14, 15, 16, 17, 18, 19], "SE": [79],
#         "TO": [63]
#     }
#     return area_codes.get(state.upper(), [])

# get_area_code_udf = udf(get_area_code, ArrayType(IntegerType()))

# client_location = client_location.withColumn("cd_area", get_area_code_udf(client_location.uf))
# display(client_location)

# COMMAND ----------

num_clients = clients.count()
num_rows_antenna = 13_000_000_000
num_rows_antenna_per_client = num_rows_antenna / num_clients

# Calculate the volume of traffic for the morning period, assuming 10% of the total monthly traffic is used per day.
volume_traffic_morning = (qt_GB_month / 30) * 0.1

# Assuming the number of rows is uniformly distributed throughout the day, with variations only in tx_uplink and tx_down.
num_sess = (end_time_home - begin_time_home) * 100
morning_tx_down = (volume_traffic_morning * 1024 **3) / (num_sess * 50)
morning_tx_up = 0.10 * morning_tx_down


# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import ArrayType, IntegerType

# def get_area_code(state):
#     area_codes = {
#         "AC": [68], "AL": [82], "AP": [96], "AM": [92, 97], "BA": [71, 73, 74, 75, 77],
#         "CE": [85, 88], "DF": [61], "ES": [27, 28], "GO": [62, 64], "MA": [98, 99],
#         "MT": [65, 66], "MS": [67], "MG": [31, 32, 33, 34, 35, 37, 38], "PA": [91, 93, 94],
#         "PB": [83], "PR": [41, 42, 43, 44, 45, 46], "PE": [81, 87], "PI": [86, 89],
#         "RJ": [21, 22, 24], "RN": [84], "RS": [51, 53, 54, 55], "RO": [69], "RR": [95],
#         "SC": [47, 48, 49], "SP": [11, 12, 13, 14, 15, 16, 17, 18, 19], "SE": [79],
#         "TO": [63]
#     }
#     return area_codes.get(state.upper(), [])

# get_area_code_udf = udf(get_area_code, ArrayType(IntegerType()))

# client_location1 = client_location.withColumn("cd_area", get_area_code_udf(client_location.uf))
display(client_location1)

# COMMAND ----------

client_location1.write.mode("overwrite").saveAsTable(f"{catalog_name}.{p_schema_misc}.aux_tbl_cliente_localizacao")
dbutils.notebook.exit("success")

# COMMAND ----------

#
# Generating traffic data simulating user's location and traffic data. The user's location is his home location.
# This includes session time, upload/download traffic, volume, and timestamps.
client_location1 = (
    client_location1
    .withColumn("tempo_sessao", expr(f"transform(sequence(1, {num_sess}), x -> cast(rand() * 50 + 5 as int))"))    
    .withColumn("tx_up", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  {morning_tx_up} as int))"))    
    .withColumn("tx_down", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  {morning_tx_down} as int))"))    
    #.withColumn("qt_volume", expr(f"transform(sequence(1,  {num_sess}), x -> tx_down * tempo_sessao)"))  
    .withColumn("qt_volume", expr(f"transform(sequence(1,  {num_sess}), x -> element_at(tx_down, x) * element_at(tempo_sessao, x))"))       
    .withColumn("ts_start_add", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  8 as int))"))
    .withColumn("data_ref",  to_timestamp(lit(data_ref)))
    .withColumn("data_ref_unix", unix_timestamp("data_ref"))   
    .withColumn(
     "incremented_timestamps",
        expr(
            """
            transform(  
                sequence(1, size(ts_start_add)), i ->    
                    aggregate(
                        slice(ts_start_add, 1, i),
                            data_ref_unix + 
                                if (i > 1,
                                    aggregate(
                                        slice(tempo_sessao, 1, i - 1),
                                        0,
                                        (acc, x) -> acc + x
                                    ),
                                    0
                                ),
                                (acc, x) -> acc + x
                    )
            )
            """
        )
    )       
    .withColumn("timestamp_end",  
                expr("""
                    transform(
                        sequence(1, size(ts_start_add)),
                        i -> element_at(incremented_timestamps, i) + element_at(tempo_sessao, i)
                    )
                """)
    )      
)

# COMMAND ----------

display(client_location1)

# COMMAND ----------

from pyspark.sql.functions import posexplode, col, arrays_zip, explode

client_location1_exploded = (
    client_location1.withColumn('cd_pais', lit(55)
    ).select(
        "nu_tlfn", "cd_cgi", "nu_imei_aprl", "lon_res", "lat_res",
        explode(
            arrays_zip(
                "tempo_sessao", "tx_up", "tx_down", "qt_volume",
                "ts_start_add", "incremented_timestamps", "timestamp_end"
            )
        ).alias("zipped"),
        "cd_pais", "cd_area"
    )
    .select(
        "nu_tlfn", "cd_cgi", "nu_imei_aprl", "lon_res", "lat_res",
        "zipped.*", "cd_pais", "cd_area"
    )
)

display(client_location1_exploded)

# COMMAND ----------

morning_client_antenna = client_location1_exploded.select(
    col('nu_tlfn').alias('msisdn'),
    col('tx_up').alias('tx_uplink'),
    col('tx_down').alias('tx_downlink'),
    col('qt_volume'),
    col('tempo_sessao').alias('qt_duration'),
    col('incremented_timestamps').alias('ds_start_time').cast("timestamp"),
    col('timestamp_end').alias('ds_end_time').cast("timestamp"),
    col('nu_imei_aprl').alias('num_served_imei'),
    col('cd_cgi').cast("int"),
    col('cd_area').cast("int"),
    col('cd_pais').cast("int")
    
)

display(morning_client_antenna)

# COMMAND ----------

morning_client_antenna.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{p_schema_misc}.aux_tbl_antenna_pattern")
