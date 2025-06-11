# Databricks notebook source
import dbldatagen as dg

# COMMAND ----------

dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "customer_bronze")
dbutils.widgets.text("p_data_schema", "ingestion")


p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_data_schema = dbutils.widgets.get("p_data_schema")

table_aux_tbl_produtos = f"{p_catalog}.misc.aux_tbl_produtos"
table_aux_tbl_clientes = f"{p_catalog}.misc.aux_tbl_clientes"
table_aux_tbl_servicos = f"{p_catalog}.misc.aux_tbl_servicos"

volume_path = f"/Volumes/{p_catalog}/{p_data_schema}/raw_data/customer/sva_subscriptions"




# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog $p_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists $p_catalog.$p_schema

# COMMAND ----------

from dbldatagen import FakerTextFactory, DataGenerator, fakerText, DataAnalyzer
from faker.providers import internet
from pyspark.sql.types import FloatType, StringType

# COMMAND ----------

servicos = spark.sql(f"select productid from {p_catalog}.misc.aux_tbl_servicos").collect()
servicos = [x.productid for x in servicos]

# COMMAND ----------

FakerTextIT = FakerTextFactory(locale=['pt_BR'])
data_rows = 1_000_000/10

generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=data_rows,
                     random=True,
                     )
    .withColumn('data_contratacao', 'timestamp', begin="2023-01-01 00:00:00", end="2023-12-31 00:00:00", interval="6 hour", random=True)
    .withColumn('protocol_number', 'string', text=dg.TemplateGenerator(r'KKKKKKK-dddddd'), uniqueValues=data_rows)
    .withColumn('subscribestate', 'string', values=["Ativo", "Cancelado", "Suspenso"], weights=[0.7, 0.2, 0.1])
    .withColumn('tplinha', 'string', values=["Móvel"])
    .withColumn('productid', 'string', values=servicos, random=True)
    
    )

# COMMAND ----------

# DBTITLE 1,aux_tables
df_aux_tbl_servicos = spark.read.table(table_aux_tbl_servicos)
df_aux_tbl_clientes = spark.read.table(table_aux_tbl_clientes)

# COMMAND ----------

df_sva_subscriptions = generation_spec.build()

# COMMAND ----------

display(df_sva_subscriptions)

# COMMAND ----------

from pyspark.sql.functions import broadcast, expr
df_sva_subscriptions = df_sva_subscriptions.withColumn("row_number", expr("row_number() over (order by rand())"))
df_aux_tbl_clientes = df_aux_tbl_clientes.withColumn("row_number", expr("row_number() over (order by rand())"))
df_aux_tbl_servicos = df_aux_tbl_servicos.withColumn("row_number", expr("row_number() over (order by rand())"))
df = df_sva_subscriptions.join(df_aux_tbl_clientes, on="row_number", how="inner")
df = df.join(df_aux_tbl_servicos, on="productid", how="inner")
display(df)

# COMMAND ----------

# DBTITLE 1,transformations
# Rename column
df = df.withColumnRenamed("nu_tlfn", "msisdn")

# Generate discountValue column based on the client (segmentation) and grossvalue
df = df.withColumn("discountValue", expr("""case when client = 'P' then 00 
                                                when client = 'S' then grossvalue * 0.02 
                                                when client = 'G' then grossvalue * 0.05 
                                                when client = 'PL' then grossvalue * 0.10
                                                when client = 'V' then grossvalue * 0.15 end""").cast("string"))

# Cast the column data_contratacao as string and in the format YYYYMMDDHHMMSS
df = df.withColumn("data_contratacao", expr("date_format(data_contratacao, 'yyyyMMddHHmmss')"))



# COMMAND ----------

# DBTITLE 1,Correct column order
df = df.withColumnRenamed("nu_tlfn", "msisdn")
columns = ['msisdn',
 'productid',
 'productname',
 'protocol_number',
 'spname',
 'subscribestate',
 'productaccountinggroup',
 'client',
 'servicetype',
 'tplinha',
 'grossvalue',
 'company',
 'taxpis',
 'taxcofins',
 'taxiss',
 'discountvalue',
 'data_contratacao']
df = df.select(columns)

# COMMAND ----------

# DBTITLE 1,PK validation
# Validate PK
PK = ["msisdn", "productid", "data_contratacao"]
assert data_rows == df.select(PK).distinct().count()

# COMMAND ----------

df.repartition(2).write.format("parquet").mode("overwrite").option("path", volume_path).save()

# COMMAND ----------

# DBTITLE 1,Overwrite table
# Arrumar os decimais da coluna taxispis.
#df.write.format("delta").mode("overwrite").saveAsTable(f"{p_catalog}.{p_schema}.sva_subscriptions")
