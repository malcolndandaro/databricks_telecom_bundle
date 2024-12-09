# Databricks notebook source
p_catalog = spark.conf.get("confs.p_catalog")
p_schema = "customer_gold"
p_table = "dim_sva"

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

aux_tbl_sva = f"{p_catalog}.misc.aux_tbl_servicos"
df_aux_tbl_produtos = spark.read.table(aux_tbl_sva)


# COMMAND ----------

schema_ddl = """
    SK_DIM_SVA LONG PRIMARY KEY COMMENT 'Chave Primária',
    GOLD_TS TIMESTAMP COMMENT 'Timestamp de Ingestão da Linha',
    PRODUCTID STRING COMMENT 'Identificador do Produto',
    PRODUCTNAME STRING COMMENT 'Nome do Produto',
    SPNAME STRING COMMENT 'Provedor do Serviço',
    PRODUCTACCOUNTINGGROUP STRING COMMENT 'Informação do Grupo do Produto',
    SERVICETYPE STRING COMMENT 'Tipo do Serviço: Bundle / Avulso',
    GROSSVALUE INTEGER COMMENT 'Valor Bruto do Serviço',
    COMPANY STRING COMMENT 'Empresa',
    TAXPIS DECIMAL(7,2) COMMENT 'Percentual de PIS',
    TAXCOFINS DECIMAL(7,2) COMMENT 'Percentual de COFINS',
    TAXISS DECIMAL(7,2) COMMENT 'Percentual de ISS',
    __START_AT TIMESTAMP COMMENT 'Inicio Vigencia',
    __END_AT TIMESTAMP COMMENT 'Fim Vigencia'
"""

# COMMAND ----------

import dlt


@dlt.view()
def sva_snapshot():
    return (
        spark.read.table(aux_tbl_sva)
        .select(
            [
                "PRODUCTID",
                "PRODUCTNAME",
                "SPNAME",
                "PRODUCTACCOUNTINGGROUP",
                "SERVICETYPE",
                "GROSSVALUE",
                "COMPANY",
                "TAXPIS",
                "TAXCOFINS",
                "TAXISS",
            ]
        )
        .withColumn("SK_DIM_SVA", monotonically_increasing_id())
        .withColumn("GOLD_TS", current_timestamp())
    )


dlt.create_streaming_table(name=p_table, comment=p_table, schema=schema_ddl)


dlt.apply_changes_from_snapshot(
    target=p_table,
    source="sva_snapshot",
    keys=["productid"],
    stored_as_scd_type="2",
    track_history_except_column_list=["SK_DIM_SVA", "GOLD_TS"],
)
