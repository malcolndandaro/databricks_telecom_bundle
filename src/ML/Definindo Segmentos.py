# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array
import mlflow
from mlflow.models.signature import infer_signature
import mlflow
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import FeatureLookup

# COMMAND ----------

dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")
dbutils.widgets.text("date_param","2024-09-01", "Data parâmetro")
catalog_name = dbutils.widgets.get("catalog_name")
date_param = dbutils.widgets.get("date_param")

# COMMAND ----------

import mlflow
from databricks.feature_store import FeatureStoreClient
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")

model_name = f"{catalog_name}.customer_gold.microseg_model"


client = MlflowClient()

model_versions = client.search_model_versions(f"name = '{model_name}'")
latest_version = 1
if model_versions:
    latest_version = max([int(mv.version) for mv in model_versions])
    print(f"The latest version = {latest_version}")
else:
    print("No model versions found.")


# COMMAND ----------

import mlflow

# Load the model
microseg_model = mlflow.spark.load_model(f"models:/{model_name}/{latest_version}")

# Log the cluster centers as a parameter
# cluster_centers = microseg_model.stages[-1].clusterCenters()
# centers = mlflow.log_param("clusterCenters", cluster_centers)
centers = microseg_model.stages[-1].clusterCenters()

# COMMAND ----------

#df_interesse = spark.table(f"{catalog_name}.customer_gold.fs_interesse")

# COMMAND ----------


df_interesse = spark.sql(f"select * from {catalog_name}.customer_gold.fs_interesse where partition_date='{date_param}'")

# COMMAND ----------

display(df_interesse)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# Combinar todas as features em um único vetor
assembler = VectorAssembler(
    inputCols=['lat_decimal', 'lon_decimal', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11', 'col12', 'col13', 'col14', 'col15'],
    outputCol='features'
)


data = assembler.transform(df_interesse)

# COMMAND ----------

display(data)

# COMMAND ----------

result = microseg_model.transform(data)


# COMMAND ----------

display(result)

# COMMAND ----------

import numpy as np
import pandas as pd 

centroids_df = pd.DataFrame(centers)

# COMMAND ----------

centroids_df.columns = ['lat_decimal', 'lon_decimal', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11', 'col12', 'col13', 'col14', 'col15']
display(centroids_df)

# COMMAND ----------

broadcast_centroids = spark.sparkContext.broadcast(centroids_df)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
import pandas as pd
import numpy as np

# Assume centroids is a broadcast variable containing the centroids DataFrame
broadcast_centroids = spark.sparkContext.broadcast(centroids_df)

@pandas_udf(StructType([StructField("nr_tlfn", StringType(), True), StructField("prediction", DoubleType(), True)] + [StructField(f"d_{i}", DoubleType(), True) for i in range(90)]), PandasUDFType.GROUPED_MAP)
def calculate_distances_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    # Access the broadcasted centroids
    centroids = broadcast_centroids.value
    
    # Extract coordinate columns
    coord_columns = ['lat_decimal', 'lon_decimal', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11', 'col12', 'col13', 'col14', 'col15']  
    data_points = pdf[coord_columns].values
    centroids_array = centroids[coord_columns].values
    
    # Calculate distances
    squared_diff = (centroids_array[:, np.newaxis, :] - data_points) ** 2
    distances = np.sqrt(np.sum(squared_diff, axis=-1)).T
    
    # Create a DataFrame with the results
    result_df = pd.DataFrame(
        distances,
        columns=[f"d_{i}" for i in range(len(centroids))]
    )
    
    # Include nr_tlfn and existing_prediction in the DataFrame
    result_df["nr_tlfn"] = pdf["nr_tlfn"].values
    result_df["prediction"] = pdf["prediction"].values
    
    return result_df

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf, row_number, col, PandasUDFType
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField
from pyspark.sql import Window


from pyspark.sql.functions import struct

Y_cols = ["lat_decimal", 
          "lon_decimal", 
          "col1", 
          "col2",  
          "col3",
          "col4",
          "col5",
          "col6",
          "col7",
          "col8",
          "col9",
          "col10",
          "col11",
          "col12",
          "col13",
          "col14",
          "col15"]

window_spec = Window.partitionBy('nr_tlfn').orderBy(col('prediction'))
df_limited = result.withColumn('row_number', row_number().over(window_spec)) \
                     .filter(col('row_number') <= 100000) \
                     .drop('row_number')

result_df = df_limited.groupBy('nr_tlfn').apply(calculate_distances_udf)
display(result_df)


# COMMAND ----------

from pyspark.sql.functions import array, lit, to_date

# Combine columns d_0 to d_89 into a single array column
result_df = result_df.withColumn("distances_to_centers", array([col(f"d_{i}") for i in range(90)]))

# Drop the original distance columns
result_df = result_df.drop(*[f"d_{i}" for i in range(90)])



display(result_df)



# COMMAND ----------

interesse = result_df.select(
    col('nr_tlfn').alias('msisdn'),  
    to_date(lit(date_param), "yyyy-MM-dd").alias('partition_date'),  
    'prediction',
    'distances_to_centers'                                              
)

display(interesse)

# COMMAND ----------

from pyspark.sql.functions import concat, to_date, lit

interesse = result_df.select(
    col('nr_tlfn').alias('msisdn'),  
    to_date(lit(date_param), "yyyy-MM-dd").alias('partition_date'),  
    'prediction',
    'distances_to_centers'                                              
)

display(interesse)

# COMMAND ----------

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.customer_bronze.tbl_aux_interesse (
    msisdn STRING,
    partition_date DATE,
    distances_to_interesse ARRAY<DOUBLE>
    )
    PARTITIONED BY (partition_date)
"""
)

# COMMAND ----------

interesse.printSchema()

# COMMAND ----------

#interesse = interesse.dropDuplicates(["partition_date"])
interesse.write.format("delta"
                       ).mode("overwrite"
                       ).option("mergeSchema", "true"
                       ).partitionBy("partition_date"
                       ).saveAsTable(f"{catalog_name}.customer_silver.tbl_aux_interesse")

# COMMAND ----------

display(interesse)
