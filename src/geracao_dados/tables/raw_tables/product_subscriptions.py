# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "customer_bronze")
dbutils.widgets.text("p_data_schema", "ingestion")
dbutils.widgets.text("data_size", "small", "Data size (small=5%, medium=25%, large=100%)")

p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_data_schema = dbutils.widgets.get("p_data_schema")
data_size = dbutils.widgets.get("data_size")

table_aux_tbl_produtos = f"{p_catalog}.misc.aux_tbl_produtos"
table_aux_tbl_clientes = f"{p_catalog}.misc.aux_tbl_clientes"

volume_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/customer/product_subscriptions"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog $p_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists $p_catalog.$p_schema

# COMMAND ----------

import dbldatagen as dg
from dbldatagen import FakerTextFactory, DataGenerator, fakerText, DataAnalyzer
from faker.providers import internet
from pyspark.sql.types import FloatType, StringType

# COMMAND ----------

# Import the data size utility functions
%run ../aux_functions/data_size_utils

# COMMAND ----------

FakerTextIT = FakerTextFactory(locale=['pt_BR'])
original_rows = 2000000
data_rows = calculate_data_rows(original_rows, data_size)

generation_spec = (
    dg.DataGenerator(name='synthetic_data', 
                     rows=data_rows,
                     random=True,
                     sparkSession=spark
                     )
    .withColumn('dt_prmr_atvc_lnha', 'date', begin="2015-01-01", end="2024-09-01", random=True)
    .withColumn('base_dt_dstv_lnha', 'date', expr="dateadd(day, (round(rand()*1000) * round(rand()*10)), dt_prmr_atvc_lnha)", omit=True)
    .withColumn('dt_dstv_lnha', 'date', baseColumn="base_dt_dstv_lnha", expr="case when base_dt_dstv_lnha <= '2024-09-01' then base_dt_dstv_lnha else null end")
    .withColumn('id_prdt', 'smallint', uniqueValues=30, random=True)
    .withColumn('id_estd_lnha', 'smallint', baseColumn="dt_dstv_lnha", expr="case when dt_dstv_lnha is null then 1 else 0 end")
    .withColumn('id_disp_xdsl', 'tinyint', values=[1,0], weights=[1,3], random=True)
    .withColumn('base_id_disp_fttc', 'tinyint', values=[1,0], weights=[1,3], omit=True)
    .withColumn("id_disp_fttc", 'tinyint', baseColumns=["base_id_disp_fttc","id_disp_xdsl"], expr="case when id_disp_xdsl == 0 then base_id_disp_fttc else 0 end")
    .withColumn('base_id_disp_ftth', 'tinyint', values=[1,0], weights=[1,3], omit=True)
    .withColumn("id_disp_ftth", 'tinyint', baseColumns=["id_disp_fttc","id_disp_xdsl", "base_id_disp_ftth"], expr="case when id_disp_xdsl == 0 and id_disp_fttc == 0 then base_id_disp_ftth else 0 end")
    .withColumn('fl_plno_dscn', 'tinyint', values=[1,0], weights=[1,2])
    .withColumn('fl_debt_autm', 'tinyint', values=[1,0], weights=[1,4])
    .withColumn('fl_cnta_onln', 'tinyint', values=[1,0], weights=[2,1])
    .withColumn('fl_plno_ttlr', 'tinyint', values=[1,0], weights=[1,0.2])
    .withColumn('fl_vivo_total', 'int', values=[1,0], weights=[0.05,1])
    .withColumn('dt_trca_aprl', 'date', begin="2018-01-01", end="2024-09-01", interval="1 day", random=True)
    .withColumn('dt_ini_plno', 'date', begin="2016-01-01", end="2024-09-01", interval="1 day", random=True)
    )

# COMMAND ----------

# DBTITLE 1,aux_tables
df_aux_tbl_produtos = spark.read.table(table_aux_tbl_produtos)
df_aux_tbl_clientes = spark.read.table(table_aux_tbl_clientes)

# COMMAND ----------

df_product_subscriptions = generation_spec.build()

# COMMAND ----------

from pyspark.sql.functions import broadcast
df_product_subscriptions = generation_spec.build()
df_product_subscriptions = df_product_subscriptions.join(broadcast(df_aux_tbl_produtos), on="id_prdt", how="inner")

display(df_product_subscriptions)

# COMMAND ----------

from pyspark.sql.functions import row_number, expr

df_product_subscriptions = df_product_subscriptions.withColumn("row_number", expr("row_number() over (order by rand())"))
df_aux_tbl_clientes = df_aux_tbl_clientes.withColumn("row_number", expr("row_number() over (order by rand())"))

# COMMAND ----------

df = df_product_subscriptions.join(df_aux_tbl_clientes, on="row_number", how="inner")

# COMMAND ----------

# DBTITLE 1,Correct column order

columns = ['nu_tlfn',
 'dt_prmr_atvc_lnha',
 'dt_dstv_lnha',
 'nu_doct',
 'id_prdt',
 'ds_prdt',
 'ds_plno',
 'id_estd_lnha',
 'cd_ddd',
 'uf',
 'no_lgrd',
 'no_imovel',
 'no_brro',
 'nu_cep',
 'no_mnco',
 'cd_ibge_mnco',
 'id_disp_xdsl',
 'id_disp_fttc',
 'id_disp_ftth',
 'fl_plno_dscn',
 'fl_debt_autm',
 'fl_cnta_onln',
 'fl_plno_ttlr',
 'nu_imei_aprl',
 'ds_modl_orig_aprl',
 'fl_vivo_total',
 'dt_trca_aprl',
 'dt_ini_plno',
 'user_id']
df = df.select(columns)

# COMMAND ----------

# DBTITLE 1,PK validation
# Validate PK
PK = ["nu_tlfn", "nu_doct", "user_id", "id_prdt", "dt_prmr_atvc_lnha"]
assert data_rows == df.select(PK).distinct().count()

# COMMAND ----------

df.repartition(2).write.format("parquet").mode("overwrite").option("path", volume_path).save()

# COMMAND ----------

# DBTITLE 1,Overwrite table
##df.write.format("delta").mode("overwrite").saveAsTable(f"{p_catalog}.{p_schema}.product_subscriptions")
