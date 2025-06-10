# Databricks notebook source
dbutils.widgets.text("p_schema", "misc")
dbutils.widgets.text("p_table", "dim_customer")

# COMMAND ----------

p_catalog = spark.conf.get("confs.p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_table = dbutils.widgets.get("p_table")

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

aux_tbl_clientes = f"{p_catalog}.misc.aux_tbl_clientes"
df_aux_tbl_clientes = spark.read.table(aux_tbl_clientes)


# COMMAND ----------

schema_ddl = """
    SK_DIM_CUSTOMER BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY COMMENT 'Chave Primária',
    ROW_INGESTION_TIMESTAMP TIMESTAMP COMMENT 'Timestamp de Ingestao da Linha',
    NU_DOCT STRING COMMENT 'Número do Documento',
    NU_TLFN STRING COMMENT 'Numero do telefone do usuario',
    USER_ID STRING COMMENT 'ID do Usuário',
    NU_IMEI_APRL STRING COMMENT 'Imei do Usuario naquele momento',
    DS_MODL_ORIG_APRL STRING COMMENT 'Modelo do Aparelho',
    NO_LGRD STRING COMMENT 'Nome da Rua',
    NO_IMOVEL STRING COMMENT 'Número do Imóvel',
    NO_BRRO STRING COMMENT 'Bairro',
    NU_CEP STRING COMMENT 'Código Postal',
    NO_MNCO STRING COMMENT 'Cidade',
    CD_IBGE_MNCO STRING COMMENT 'Código IBGE da Cidade',
    UF STRING COMMENT 'Estado',
    CD_DDD SHORT COMMENT 'Código de Área',
    CLIENT STRING COMMENT 'Código do Cliente, se na classificação de PURPURA, SILVER, GOLD, PLATINUM, VIP',
    __START_AT TIMESTAMP COMMENT 'Inicio Vigencia',
    __END_AT TIMESTAMP COMMENT 'Fim Vigencia'
"""

# COMMAND ----------

import dlt
from pyspark.sql.functions import monotonically_increasing_id, xxhash64, col

@dlt.view()
def clientes_cdf():
  return (
      spark
      .readStream
      .option("readChangeFeed", "True")
      .table(aux_tbl_clientes)
      .withColumn("ROW_INGESTION_TIMESTAMP", current_timestamp())
      #.withColumn("SK_DIM_CUSTOMER", xxhash64(col("nu_doct")))
  )


dlt.create_streaming_table(
    name = p_table,
    comment = "dim_customer",
    schema=schema_ddl
)

dlt.apply_changes(
  target = p_table,
  source = "clientes_cdf",
  keys = ["NU_DOCT"],
  sequence_by=col("last_update"),
  apply_as_deletes = expr("_change_type = 'delete'"),
  except_column_list = ["_change_type", "_commit_version", "_commit_timestamp", "last_update"],
  stored_as_scd_type = "2"
)

