# Databricks notebook source
# MAGIC %md
# MAGIC # Introdução
# MAGIC
# MAGIC Neste projeto necessitamos de um recurso de busca de coordenadas por proximidade.
# MAGIC Para processar um grande volume de dados esse recurso precisa ser rápido, por isso, usaremos a biblioteca FAISS que usa algoritmos sofisticados de agrupamento k-means e quantização de produtos para busca de similaridades. É muito utilizados em LLMs mas atende perfeitamente as nossas necessidades neste projeto

# COMMAND ----------

# MAGIC %pip install faiss-cpu==1.8.0.post1  # for CPU-only version
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col, udf, cast
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType, StringType, IntegerType
import math
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType



# COMMAND ----------

dbutils.widgets.dropdown('reset', 'False', ['False', 'True'], 'Reset Auxiliary Data')
dbutils.widgets.text('num_grp_col', '100', 'Num. Columns Groups')

# COMMAND ----------

num_grp_col = int(dbutils.widgets.get('num_grp_col'))
reset = dbutils.widgets.get('reset') == 'True'

# COMMAND ----------

# DBTITLE 1,Defining Center Clustering
address_df = (spark
              .read.table(f'{catalog_name}.misc.aux_enderecos')
            )

dim = address_df.select(
    F.min(col('lat')), 
    F.max(col('lat')), 
    F.min(col('lon')),
    F.max(col('lon'))
).collect()

min_lat = dim[0][0]
max_lat = dim[0][1]
min_lon = dim[0][2]
max_lon = dim[0][3]

dist_lat = max_lat - min_lat
dist_lon = max_lon - min_lon

size_grp_lan = round(dist_lat / num_grp_col, 1)
size_grp_lon = round(dist_lon / num_grp_col, 1)


# COMMAND ----------

print(min_lat, max_lat, min_lon, max_lon, size_grp_lan, size_grp_lon)

# COMMAND ----------

address_df = (
    address_df
        .withColumn('grp_lat', ((col('lat') - min_lat) / size_grp_lan).cast("integer"))
        .withColumn('grp_lon', ((col('lon') - min_lon) / size_grp_lon).cast("integer"))
        .withColumn('grp_tmp', col('grp_lat') * num_grp_col + col('grp_lon'))
        .withColumn('grp', col('grp_tmp').cast('int'))
        .drop('grp_lat', 'grp_lon', 'grp_tmp')

)

# COMMAND ----------

address_df.write.mode('overwrite').saveAsTable(f'{catalog_name}.misc.aux_enderecos_grp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.misc.aux_enderecos_grp') limit 100

# COMMAND ----------

erb_df = spark.read.table(f'{catalog_name}.misc.aux_erb_coord')
display(erb_df)

# COMMAND ----------

import numpy as np

# Assuming the dataframe is named `company_df` and has columns 'lat' and 'lon'

pandas_df = erb_df.select("NumEstacao", "lat_decimal", "lon_decimal").toPandas()
coordinates = pandas_df[["lat_decimal", "lon_decimal"]].values.astype(np.float32)
ids = pandas_df["NumEstacao"].values

# COMMAND ----------

import faiss
import numpy as np

dimension = 2  # since we're dealing with 2D coordinates
quantizer = faiss.IndexFlatL2(dimension)  # d is the dimension of your vectors
index = faiss.IndexIVFFlat(quantizer, dimension, 10000, faiss.METRIC_L2)

# Assuming `coordinates` is a large numpy array of shape (N, 2)
batch_size = 10000  # Define a suitable batch size
num_batches = (coordinates.shape[0] + batch_size - 1) // batch_size

# Train the index in batches
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = min((i + 1) * batch_size, coordinates.shape[0])
    index.train(coordinates[start_idx:end_idx])

# Add vectors to the index in batches
for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = min((i + 1) * batch_size, coordinates.shape[0])
    index.add(coordinates[start_idx:end_idx])

# COMMAND ----------

# Ensure the index is empty before creating the IndexIDMap
index.reset()
id_map = faiss.IndexIDMap(index)
id_map.add_with_ids(coordinates, ids)

# COMMAND ----------

print(len(coordinates))

# COMMAND ----------

print(len(ids))

# COMMAND ----------

k = 3
# query_coordinate = np.array([ [-9.601388888888888, -35.770555555555555]], dtype=np.float32)
query_coordinate = np.array([ [-4.559266666666666,-38.92037777777778]], dtype=np.float32)

# COMMAND ----------

distances, found_ids = id_map.search(query_coordinate, k)

# COMMAND ----------

print(found_ids)

# COMMAND ----------

for i in range(k):
    print(f"Neighbor {i+1}: ID {found_ids[0][i]}, Distance: {distances[0][i]}")
    # If you need the actual coordinates, you can look them up in your original DataFrame
    if( found_ids[0][i] >= 0):
        neighbor_coords = erb_df.filter(col("NumEstacao") == found_ids[0][i]).select("lat_decimal", "lon_decimal").collect()[0]
        print(f"Coordinates: {neighbor_coords}")
       

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog_name || '.misc.aux_erb_coord') where NumEstacao= '1002298137'

# COMMAND ----------

print(found_ids)

# COMMAND ----------

import faiss
import pickle

# Save the index
index_file_path = f"/Volumes/{catalog_name}/misc/erbs/index_erb.bin"
faiss.write_index(index, index_file_path)

# Save the mapping of IDs
id_coord_file_path = f"/Volumes/{catalog_name}/misc/erbs/id_coord.pkl"
ids_file_path = f"/Volumes/{catalog_name}/misc/erbs/ids.pkl"
with open(id_coord_file_path, 'wb') as f:
    pickle.dump(coordinates, f)

with open(ids_file_path, 'wb') as f:
    pickle.dump(ids, f)



# COMMAND ----------

# MAGIC %md
# MAGIC
