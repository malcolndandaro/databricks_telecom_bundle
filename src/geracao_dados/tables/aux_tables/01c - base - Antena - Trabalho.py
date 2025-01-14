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
dbutils.widgets.text("arrival_time", "9","Hora de chegada")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data de Referência")
dbutils.widgets.text("qt_GB_month", "25","GB p/ Mês")
dbutils.widgets.text("num_sess", "300","Número de sessões deslocamento")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data Referência")
dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")

data_ref            = dbutils.widgets.get("data_ref")
departure_time      = int(dbutils.widgets.get("departure_time"))
arrival_time        = int(dbutils.widgets.get("arrival_time"))
qt_GB_month         = int(dbutils.widgets.get("qt_GB_month"))

data_ref            = dbutils.widgets.get("data_ref")
num_sess            = (departure_time - arrival_time) * 100
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga dos trajetos dos clientes ao trabalho
# MAGIC
# MAGIC Carregando trajeto percorrido pelo cliente:
# MAGIC https://adb-6120542968195864.4.azuredatabricks.net/?o=6120542968195864#notebook/316131127233238/command/316131127233243
# MAGIC
# MAGIC

# COMMAND ----------

trajeto_cliente = spark.read.table(f'{catalog_name}.misc.aux_tbl_trajeto')


# COMMAND ----------

n_cli = spark.sql(f"select count(*) as num_clients from {catalog_name}.misc.aux_tbl_clientes").collect()
num_clients = n_cli[0]['num_clients']
num_rows_antenna = 13_000_000_000
num_rows_antenna_per_client = num_rows_antenna / num_clients

volume_traffic_trabalho = (qt_GB_month / 30) * 0.60

trabalho_tx_down = (volume_traffic_trabalho * 1024 **3) / (num_sess * 50)
trabalho_tx_up = 0.15 * trabalho_tx_down

# COMMAND ----------

from pyspark.sql.functions import expr

# Pegando a última posição do trajeto
clnt_local_trabalho = trajeto_cliente.withColumn(
    "cd_cgi_ultimo", expr("element_at(cd_cgi_percurso, -1)")
).drop("cd_cgi_percurso", "num_sess", "hora_saida")

display(clnt_local_trabalho)

# COMMAND ----------


client_trabalho = clnt_local_trabalho.withColumn("tempo_sessao",
                                                  expr(f"transform(sequence(1, {num_sess}), x -> cast(rand() * 50 + 5 as int))")    
    ).withColumn("tx_up", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  {trabalho_tx_up} as int))")
    ).withColumn("tx_down", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  {trabalho_tx_down} as int))")
    ).withColumn("qt_volume", expr(f"transform(sequence(1,  {num_sess}), x -> element_at(tx_down, x) * element_at(tempo_sessao, x))")
    ).withColumn("ts_start_add", expr(f"transform(sequence(1,  {num_sess}), x -> cast(rand() *  8 as int))")
    ).withColumn("data_ref",  to_timestamp(lit(data_ref))
    ).withColumn("data_ref_unix", unix_timestamp("data_ref") + (arrival_time * 60 * 60)
    ).withColumn(
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
    ).withColumn("timestamp_end",  
                expr("""
                    transform(
                        sequence(1, size(ts_start_add)),
                        i -> element_at(incremented_timestamps, i) + element_at(tempo_sessao, i)
                    )
                """)
    )  



# COMMAND ----------

display(client_trabalho)

# COMMAND ----------

from pyspark.sql.functions import posexplode, col, arrays_zip, explode

clnt_local_trabalho_exploded = (
    client_trabalho.withColumn('cd_pais', lit(55)
    ).withColumn('data_ref', unix_timestamp(col("data_ref"))
    ).select(
        "msisdn", "cd_cgi_ultimo", "num_served_imei",
        explode(
            arrays_zip(
                "tempo_sessao", "tx_up", "tx_down", "qt_volume",
                "ts_start_add", "incremented_timestamps", "timestamp_end"
            )
        ).alias("zipped"),
        "cd_pais", "cd_area", "data_ref"
    )
    .select(
        "msisdn", col("cd_cgi_ultimo").alias("cd_cgi"), "num_served_imei", 
        "zipped.*", "cd_pais", "cd_area", "data_ref"
    )
)

display(clnt_local_trabalho_exploded)

# COMMAND ----------

clnt_local_trabalho_antenna = clnt_local_trabalho_exploded.select(
    col('msisdn'),
    col('tx_up').alias('tx_uplink'),
    col('tx_down').alias('tx_downlink'),
    col('qt_volume'),
    col('tempo_sessao').alias('qt_duration'),
    col('incremented_timestamps').alias('ds_start_time').cast("timestamp"),
    col('timestamp_end').alias('ds_end_time').cast("timestamp"),
    col('num_served_imei'),
    col('cd_cgi').cast("int"),
    col('cd_area').cast("int"),
    col('cd_pais').cast("int")
    
)




display(clnt_local_trabalho_antenna)

# COMMAND ----------

clnt_local_trabalho_antenna.write.mode("append").saveAsTable(f"{catalog_name}.misc.aux_tbl_antenna_pattern")
