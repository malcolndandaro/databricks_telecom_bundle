# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "billing_bronze")
dbutils.widgets.text("p_data_schema", "ingestion")
dbutils.widgets.text("data_size", "small", "Data size (small=5%, medium=25%, large=100%)")

p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_data_schema = dbutils.widgets.get("p_data_schema")
data_size = dbutils.widgets.get("data_size")

table_aux_tbl_produtos = f"{p_catalog}.misc.aux_tbl_produtos"
table_product_subscriptions = f"{p_catalog}.customer_bronze.product_subscriptions"
table_sva_subscriptions = f"{p_catalog}.customer_bronze.sva_subscriptions"

volume_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/billing/invoicing"

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
from pyspark.sql.functions import col, min, floor, months_between, lit
from datetime import datetime, timedelta

# Import the data size utility functions
%run ../aux_functions/data_size_utils

# COMMAND ----------

FakerTextIT = FakerTextFactory(locale=['pt_BR'])
original_rows = 2000000
data_rows = calculate_data_rows(original_rows, data_size)

generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=data_rows,
                     random=True,
                     )
    .withColumn('dt_ciclo_rcrg', 'date', begin="2016-01-01", end="2024-09-01", interval="1 day")
    .withColumn('dt_vncmt', 'tinyint', values=[1,3,5,8,9,10,15,20,22,27])
    .withColumn('vl_rcrg', 'decimal(18,2)', minValue=0, maxValue=200, random=True)
    .withColumn('ds_cnl_rcrg', 'string', values=["BANCARIO", "ELETRONICO"])
    .withColumn('dt_pgto', 'date', baseColumns=["dt_ciclo_rcrg", "dt_vncmt"], expr="to_date(concat(substring(dt_ciclo_rcrg, 0, 8), lpad(dt_vncmt, 2, 0)), 'yyyy-MM-dd')") # data pagamento
    
    )

# COMMAND ----------

# Reading directly from parquet as those tables will be ingested in later steps
df_sva_subscriptions = spark.read.format("parquet").option("path",f"/Volumes/{p_catalog}/ingestion/raw_data/customer/sva_subscriptions").load()
df_sva_subscriptions = df_sva_subscriptions.withColumn("discountvalue", col("discountvalue").cast(FloatType()))
df_product_subscriptions = spark.read.format("parquet").option("path",f"/Volumes/{p_catalog}/ingestion/raw_data/customer/product_subscriptions").load()
df_aux_tbl_produtos = spark.read.table(table_aux_tbl_produtos)

# COMMAND ----------

# DBTITLE 1,aux_tables
#df_sva_subscriptions = spark.read.table(table_sva_subscriptions)
#df_product_subscriptions = spark.read.table(table_product_subscriptions)
df_aux_tbl_produtos = spark.read.table(table_aux_tbl_produtos)

# COMMAND ----------

df_invoicing = generation_spec.build()

# COMMAND ----------

from pyspark.sql.functions import date_format, trunc

# COMMAND ----------

from pyspark.sql.functions import broadcast, expr, col, row_number
from pyspark.sql.window import Window

df_product_subscriptions = df_product_subscriptions
df = df_invoicing.join(df_product_subscriptions, 
                       how="inner")

columns = df.columns
df_aux_tbl_produtos = df_aux_tbl_produtos.drop("ds_prdt").drop("ds_plno")
df = df.join(df_aux_tbl_produtos, on="id_prdt", how="inner").select(columns + ["gross"])



# COMMAND ----------

if spark.catalog.tableExists(f"{p_catalog}.misc.aux_tbl_invoicing"):
    pass
else:
    df.write.mode("overwrite").format("delta").saveAsTable(f"{p_catalog}.misc.aux_tbl_invoicing")

# COMMAND ----------

df = spark.read.table(f"{p_catalog}.misc.aux_tbl_invoicing")
print(df.select("nu_tlfn", "dt_ciclo_rcrg").distinct().count())

# COMMAND ----------

columns = ['nu_doct',
 'ds_prdt',
 'ds_plno',
 'cd_ddd',
 'uf',
 'no_lgrd',
 'no_imovel',
 'no_brro',
 'nu_cep',
 'no_mnco',
 'cd_ibge_mnco',
 'dt_ciclo_rcrg',
 'dt_vncmt',
 'vl_ftra',
 'vl_rcrg',
 'ds_cnl_rcrg',
 'dt_pgto',
 'user_id']

# COMMAND ----------

df = df.select([col(c).alias(f"clientes_{c}") for c in df.columns])
df_sva_subscriptions = df_sva_subscriptions.select([col(c).alias(f"servicos_{c}") for c in df_sva_subscriptions.columns])
df = df.alias("clientes").join(df_sva_subscriptions.alias("servicos"), df["clientes_nu_tlfn"] == df_sva_subscriptions["servicos_msisdn"], how="left")
df.count()

# COMMAND ----------

df.createOrReplaceTempView("invoicing")

# COMMAND ----------

df = spark.sql("""
select
  (
    (servicos_grossvalue) * 1 - int(servicos_discountvalue)
  ) as valor_total_servicos,
  case
    when clientes_ds_prdt != 'Pré Pago' then (nvl(valor_total_servicos,0) + clientes_gross)
    when clientes_ds_prdt = 'Pré Pago' then nvl(valor_total_servicos,0) + clientes_gross
  end as vl_ftra,
  (
    case
      when 
      clientes_ds_prdt = 'Pré Pago'
      and clientes_ds_plno like '%Recarga%'
      or clientes_ds_plno like '%Pré-pago%' then clientes_gross
      else null
    end
  ) vl_rcrg,
  case when vl_rcrg is null then null else clientes_ds_cnl_rcrg end clientes_ds_cnl_rcrg,
  *
except
  (clientes_vl_rcrg, clientes_ds_cnl_rcrg)
from
  invoicing

               """)

# COMMAND ----------

columns = ["clientes_nu_doct", "clientes_ds_prdt", "clientes_ds_plno", "clientes_cd_ddd", "clientes_uf", "clientes_no_lgrd", "clientes_no_imovel", "clientes_no_brro", "clientes_nu_cep", "clientes_no_mnco", "clientes_cd_ibge_mnco", "clientes_dt_ciclo_rcrg", "clientes_dt_vncmt", "vl_ftra", "vl_rcrg", "clientes_ds_cnl_rcrg", "clientes_dt_pgto", "clientes_user_id"]

# COMMAND ----------

df = df.select([col(c).alias(c.replace("clientes_", "")) for c in columns])

# COMMAND ----------

# DBTITLE 1,PK validation
# Validate PK
PK = ["nu_doct", "dt_ciclo_rcrg"]
df.select(PK).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Generate multiple appends to simulate around 25M lines per batch/day

# COMMAND ----------


# Get the minimum value of dt_ciclo_rcrg
min_date = df.agg(min("dt_ciclo_rcrg")).collect()[0][0]

# Create the new column representing the group of each 2 months
df = df.withColumn(
    "group_2_months",
    floor(months_between(col("dt_ciclo_rcrg"), lit(min_date)) / 2)
)

# COMMAND ----------

group_2_months_list = [row["group_2_months"] for row in df.select("group_2_months").orderBy("group_2_months").distinct().collect()]
group_2_months_list.sort()

# COMMAND ----------


start_date = datetime(2024, 9, 1)
date_dict = {i: (start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(len(group_2_months_list))}

# COMMAND ----------

date_dict

# COMMAND ----------

dbutils.fs.rm(volume_path, recurse=True)

# COMMAND ----------

for k,v in date_dict.items():
    df.filter(f"group_2_months = '{k}'").drop("group_2_months").write.format("parquet").mode("append").option("path", f"{volume_path}").save()

# COMMAND ----------

#df.write.format("parquet").mode("overwrite").option("path", volume_path).save()
