# Databricks notebook source
p_catalog = spark.conf.get("confs.p_catalog")
p_schema = "customer_gold"
p_table = "dim_product"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Premissas:
# MAGIC - Nome: dim_calendario
# MAGIC - Descrição da tabela: Tabela composta por variáveis qualitativas de tempo.
# MAGIC - Tipo: SCD-1

# COMMAND ----------

# Importa bibliotecas
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType, DataType, DateType, ShortType
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

aux_tbl_produtos = f"{p_catalog}.misc.aux_tbl_produtos"
df_aux_tbl_produtos = spark.read.table(aux_tbl_produtos)


# COMMAND ----------

schema_ddl = """
    SK_DIM_PRODUCT LONG PRIMARY KEY COMMENT 'Chave Primária',
    ROW_INGESTION_TIMESTAMP TIMESTAMP COMMENT 'Timestamp de Ingestao da Linha',
    ID_PRDT INT COMMENT 'ID do Produto',
    DS_PRDT STRING COMMENT 'Descrição do Produto',
    DS_PLNO STRING COMMENT 'Descrição do Plano',
    __START_AT TIMESTAMP COMMENT 'Inicio Vigencia',
    __END_AT TIMESTAMP COMMENT 'Fim Vigencia'
"""

# COMMAND ----------

import dlt



@dlt.view()
def products_snapshot():
  return (
      spark
      .read
      #.option("readChangeFeed", "True")
      .table(aux_tbl_produtos)
      .select(["ID_PRDT", "DS_PRDT", "DS_PLNO"])
      .withColumn("SK_DIM_PRODUCT", monotonically_increasing_id())
      .withColumn("ROW_INGESTION_TIMESTAMP", current_timestamp())
  )


dlt.create_streaming_table(
    name = p_table,
    comment = p_table,
    schema=schema_ddl
)


dlt.apply_changes_from_snapshot(
  target = p_table,
  source = "products_snapshot",
  keys = ["ID_PRDT"],
  stored_as_scd_type = "2",
  track_history_except_column_list=["SK_DIM_PRODUCT","ROW_INGESTION_TIMESTAMP"]
)

