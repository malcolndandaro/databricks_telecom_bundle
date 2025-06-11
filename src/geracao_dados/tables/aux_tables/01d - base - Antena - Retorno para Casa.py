# Databricks notebook source
# MAGIC %pip install faiss-cpu==1.8.0.post1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col, lit, unix_timestamp, expr, to_timestamp, array, explode, rand, from_unixtime
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd
import numpy as np
from datetime import datetime
import torch
import faiss


# COMMAND ----------

dbutils.widgets.text("departure_time", "19","Hora de saída")
dbutils.widgets.text("arrival_time", "22","Hora de chegada")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data de Referência")
dbutils.widgets.text("qt_GB_month", "25","GB p/ Mês")
dbutils.widgets.text("displcment_number", "5","Número de deslocamentos")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data Referência")
dbutils.widgets.text("catalog_name","databricks_telecom_bundle", "Nome do catálogo")

data_ref            = dbutils.widgets.get("data_ref")
departure_time      = int(dbutils.widgets.get("departure_time"))
arrival_time        = int(dbutils.widgets.get("arrival_time"))
qt_GB_month         = int(dbutils.widgets.get("qt_GB_month"))
num_displ           = int(dbutils.widgets.get("displcment_number"))
data_ref            = dbutils.widgets.get("data_ref")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando trajetos salvos em 
# MAGIC
# MAGIC Carregando cliente e endereços gerado no notebook anterior:
# MAGIC https://adb-6120542968195864.4.azuredatabricks.net/?o=6120542968195864#notebook/316131127233238/command/4448031269110787

# COMMAND ----------

client_trajeto = spark.read.table(f'{catalog_name}.misc.aux_tbl_trajeto')

# COMMAND ----------

display(client_trajeto)

# COMMAND ----------

# DBTITLE 1,Invertendo o caminho e avançando
from pyspark.sql.functions import col, expr
from pyspark.sql.types import ArrayType

client_trajeto1 = client_trajeto.withColumn(
    "cd_cgi_ultimo", expr("element_at(cd_cgi_percurso, -1)")
    ).select(
    col("msisdn").alias("nu_tlfn"),
    col("cd_cgi_ultimo"), 
    col("cd_cgi_percurso"), 
    "num_served_imei", 
    col("cd_area").alias("cd_area"),                                         
    #col("lon").alias("lon_res"), 
    #col("lat").alias("lat_res")
)

array_columns = [
    field.name for field in client_trajeto1.schema.fields 
    if isinstance(field.dataType, ArrayType)
]

for col_name in array_columns:
    client_trajeto1 = client_trajeto1.withColumn(
        col_name, 
        expr(f"reverse({col_name})")
    )

# ## Avançando o tempo
# client_trajeto1 = client_trajeto1.withColumn("hora_saida", 
#     from_unixtime(
#         unix_timestamp(lit(data_ref)) + 
#                       ((rand() * (arrival_time - departure_time) + departure_time) * 3600)).cast("timestamp")
# )

display(client_trajeto1)

# COMMAND ----------

num_clients = client_trajeto.count()
num_rows_antenna = 13_000_000_000
num_rows_antenna_per_client = num_rows_antenna / num_clients

# Calculate the volume of traffic for the morning period, assuming 10% of the total monthly traffic is used per day.
volume_trafego_volta = (qt_GB_month / 30) * 0.15

# Assuming the number of rows is uniformly distributed throughout the day, with variations only in tx_uplink and tx_down.
num_sess = (arrival_time - departure_time) * 60
volta_casa_tx_down = (volume_trafego_volta * 1024 **3) / (num_sess * 50)
volta_casa__tx_up = 0.15 * volta_casa_tx_down


# COMMAND ----------

num_sess_per_cgi = num_sess// num_displ

# COMMAND ----------

# DBTITLE 1,Transformando deslocamento para antenas
# Transform deslocamento para:
# msisdn, tx_uplink, tx_downlink, qt_volume, qt_duration, ds_start_time, ds_end_time, nu_served_imei, cd_cgi
deslocamento = client_trajeto1.withColumn(
    "num_sess", array(*[lit(i) for i in range(num_sess)])
).withColumn(
    "cd_pais", lit(55)
).withColumn(
    "hora_saida", from_unixtime(
        unix_timestamp(lit(data_ref)) + 
                      ((rand() * (arrival_time - departure_time) + departure_time) * 3600)).cast("timestamp")
).select(
    col("nu_tlfn").alias("msisdn"),
    col("num_served_imei"),
    col("cd_area"),
    col("cd_pais").alias("cd_pais"),
    "cd_cgi_percurso",
    "num_sess",
    "hora_saida"
)

display(deslocamento)

# COMMAND ----------

deslocamento_cliente = deslocamento.withColumn(
    "sessao", explode(col("num_sess"))
    ).withColumn("cd_cgi",  col("cd_cgi_percurso")[(col("sessao") / num_sess_per_cgi).cast("bigint")]
    ).withColumn("tx_uplink", (rand() * volta_casa__tx_up).cast("int")
    ).withColumn("tx_downlink", (rand() * volta_casa_tx_down).cast("int")           
    ).withColumn("qt_duration", (rand() * 55).cast("int")
    ).withColumn("qt_volume", col("tx_downlink") * col("qt_duration")
    ).withColumn("delta_time_start", (rand() * 29).cast("int")           
    ).withColumn("delta_time_end", (rand() * 30 + 30).cast("int")           
    ).withColumn("ds_start_time", from_unixtime(unix_timestamp(col('hora_saida')) + col("delta_time_start") + (60 *  col("sessao"))).cast("timestamp")
    ).withColumn("ds_end_time", from_unixtime(unix_timestamp(col('ds_start_time')) + col("qt_duration")).cast("timestamp")
    ).drop("num_sess", "cd_cgi_percurso")   

display(deslocamento_cliente)

# COMMAND ----------

deslocamento_cliente= deslocamento_cliente.select(
    "msisdn",
    "tx_uplink",
    "tx_downlink",
    "qt_volume",
    "qt_duration",
    "ds_start_time",
    "ds_end_time",
    "num_served_imei",
    "cd_cgi",
    "cd_area",
    "cd_pais"
)

# COMMAND ----------

display(deslocamento_cliente)

# COMMAND ----------

# DBTITLE 1,Append na tablea au_tbl_antenna_pattern
deslocamento_cliente.write.mode("append").saveAsTable(f"{catalog_name}.misc.aux_tbl_antenna_pattern")
