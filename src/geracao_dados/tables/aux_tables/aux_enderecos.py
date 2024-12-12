# Databricks notebook source
# MAGIC %md 
# MAGIC # Introdução
# MAGIC
# MAGIC Este notebook usa a base de ERBs para criar a tabela de endereços.
# MAGIC Os endereços são todos fictícios, mas sua geolocalização usa como base as coordenadas das ERBs como ponto de referência.

# COMMAND ----------

# MAGIC %pip install dbldatagen
# MAGIC %pip install Faker

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

dbutils.widgets.text("p_catalog","dev", "Nome do catálogo")
dbutils.widgets.text("num_address","40", "Numero de endereços por ERB")
dbutils.widgets.text("max_distance_erb","3", "Distância máxima ao ERBs KM")


# COMMAND ----------

num_address_per_erb = int(dbutils.widgets.get("num_address"))  
max_distance_erb    = int(dbutils.widgets.get("max_distance_erb")) 
catalog_name        = dbutils.widgets.get("p_catalog")

# COMMAND ----------

df_erbs = spark.read.table(f'{catalog_name}.resource_bronze.erb_coord').filter(col("NomeEntidade") == 'VIVO')

# COMMAND ----------

# DBTITLE 1,Convertendo coordenadas em Grau Decimal
from pyspark.sql.functions import substring, col, when
df_erbs = ( df_erbs
            .withColumn('lat_deg', substring(col('Latitude'), 1, 2).cast('int'))
            .withColumn('lat_min', substring(col('Latitude'), 4, 2).cast('int'))
            .withColumn('lat_sec', substring(col('Latitude'), 6, 2).cast('int'))
            .withColumn('lat_sign', when(substring(col('Latitude'), 3, 1) == 'S', -1).otherwise(1))
            .withColumn('lon_deg', substring(col('Longitude'), 1, 2).cast('int'))
            .withColumn('lon_min', substring(col('Longitude'), 4, 2).cast('int'))
            .withColumn('lon_sec', substring(col('Longitude'), 6, 2).cast('int'))
            .withColumn('lon_sign', when(substring(col('Longitude'), 3, 1) == 'W', -1).otherwise(1))

            .withColumn('lat_decimal', (col('lat_deg') + col('lat_min') / 60 + col('lat_sec') / 3600) * col('lat_sign'))
            .withColumn('lon_decimal', (col('lon_deg') + col('lon_min') / 60 + col('lon_sec') / 3600) * col('lon_sign'))
            .drop('lat_deg', 'lat_min', 'lat_sec', 'lat_sign', 'lon_deg', 'lon_min', 'lon_sec', 'lon_sign')

)
# display(df_erbs)
df_erbs.write.mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_erb_coord")
df_erbs.write.mode("overwrite").saveAsTable(f"{catalog_name}.resource_bronze.erb_coord_dec")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, lit, explode
import pandas as pd
import numpy as np

@pandas_udf("array<struct<x:double,y:double>>")
def generate_random_array(coord: pd.Series) -> pd.Series:

    def dist_to_degree(x):
        return x / 111.11 #  1 grau é em torno de 111.11 km
    
    max_dist_erb = dist_to_degree(max_distance_erb)
    seed = 42
    np.random.seed(seed)
    num_elem = len(coord)    
    ret = np.random.uniform(-max_dist_erb, max_dist_erb, size=(num_elem, num_address_per_erb, 2))
    ret_pd = pd.Series(ret.tolist())
    return ret_pd


df_coord_enderecos = (df_erbs
                        .withColumn("random_array", explode(generate_random_array(lit(1))))
                        .withColumn("x", col("random_array.x") + col("lon_decimal"))
                        .withColumn("y", col("random_array.y") + col("lat_decimal"))
                        .select(col("NumEstacao").alias("erb_preferida"), 
                                col("NomeEntidade").alias("operadora"),
                                col("x").alias("lon"), col("y").alias("lat"),
                                col("NomeMunicipio").alias("municipio"),
                                col("CodMunicipio").alias("cod_municipio"),
                                col("SiglaUf").alias("uf"))
                                
)

# COMMAND ----------

df_coord_enderecos.write.mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_coord_enderecos")

# COMMAND ----------

# DBTITLE 1,Criando endereços fictícios com Faker e dbldatagen
import dbldatagen as dg
from dbldatagen import FakerTextFactory, DataGenerator, fakerText, DataAnalyzer

FakerTextIT = FakerTextFactory(locale=['pt_BR'])
data_rows = df_coord_enderecos.count()

generation_spec = (
    dg.DataGenerator(name='enderecos', # cidade e estado não são necessários, pois mesclaremos com dados das ERBs 
                     rows=data_rows,
                     random=True
                     )
    .withColumn('no_lgrd', 'string', text=FakerTextIT("street_address"))
    .withColumn('no_imovel', 'string', text=FakerTextIT("building_number"))
    .withColumn('no_bairro', 'string', text=FakerTextIT("bairro"))
    .withColumn('nu_cep', 'string', text=FakerTextIT("postcode")) 
    .withColumn('eh_comercial', 'string', values=['S', 'N'], weights=[1, 9])
    )

# COMMAND ----------

df_address = generation_spec.build()
display(df_address)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Add a unique identifier to each DataFrame
df_coord_enderecos_with_id = df_coord_enderecos.rdd.zipWithIndex().toDF().select(col("_2").alias("id"), col("_1.*"))
df_address_with_id         = df_address.rdd.zipWithIndex().toDF().select(col("_2").alias("id"), col("_1.*"))
# Join the DataFrames on the unique identifier
df_concatenated = df_coord_enderecos_with_id.join(df_address_with_id, on="id")

# display(df_concatenated)

# COMMAND ----------

# %sql
# drop table dev.misc.aux_enderecos

# COMMAND ----------

display(df_concatenated.count())

# COMMAND ----------


df_concatenated.write.mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_enderecos")
