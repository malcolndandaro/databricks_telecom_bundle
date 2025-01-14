# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")
dbutils.widgets.text("date_param","2024-09-01", "Data parâmetro")
catalog_name = dbutils.widgets.get("catalog_name")
date_param = dbutils.widgets.get("date_param")

# COMMAND ----------


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

# DBTITLE 1,Transformando uma Tabela Delta em Feature Store
spark.sql(f"USE CATALOG {catalog_name}")

fs = FeatureStoreClient()

feature_table = fs.create_table(
    name='customer_gold.fs_interesse',
    primary_keys=['nr_tlfn', 'cd_cgi', 'partition_date'],
    df=spark.table(f'{catalog_name}.customer_gold.fs_interesse'),
    description='Categorias navegadas pelo cliente'
)

# COMMAND ----------

# DBTITLE 1,Verificando a tabela de Features
print(fs.get_table('customer_gold.fs_interesse').description)

# COMMAND ----------

feature_lookups = [
    FeatureLookup(
        table_name='customer_gold.fs_interesse',
        feature_names=['lat_decimal','lon_decimal',
                       'col1', 'col2', 'col3', 
                       'col4', 'col5', 'col6', 
                       'col7', 'col8', 'col9', 
                       'col10', 'col11', 'col12', 
                       'col13', 'col14', 'col15'],
        lookup_key=['nr_tlfn', 'cd_cgi', 'partition_date']
    )
]

# Criar um DataFrame com apenas a coluna 'nr_tlfn' e filtrar pela data de partição
sample_df = spark.table('customer_gold.fs_interesse'
                        ).filter(f"partition_date = '{date_param}'"
                        ).select('nr_tlfn','partition_date', 'cd_cgi'
                        ).sample(fraction=0.1, seed=42)

# COMMAND ----------

display(sample_df)

# COMMAND ----------

# DBTITLE 1,Conjunto de Treinamento

training_set = fs.create_training_set(
    df=sample_df,
    feature_lookups=feature_lookups,
    label=None,  # Não há label para clusterização não supervisionada
    exclude_columns=['nr_tlfn', 'partition_date', 'cd_cgi']
)

# Carregar o DataFrame de treinamento
training_df = training_set.load_df()

display(training_df)

# COMMAND ----------

# DBTITLE 1,Preparando os dados para o Treinamento
from pyspark.ml.feature import VectorAssembler

# Combinar todas as features em um único vetor
assembler = VectorAssembler(
    inputCols=['lat_decimal', 'lon_decimal', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10', 'col11', 'col12', 'col13', 'col14', 'col15'],
    outputCol='features'
)


data = assembler.transform(training_df)


# COMMAND ----------

# DBTITLE 1,Avaliando o Modelo
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Treinar o modelo K-Means
kmeans = KMeans(k=90, featuresCol="features")
model = kmeans.fit(data)
predictions = model.transform(data)

display(predictions)

# COMMAND ----------

# Avaliar o modelo
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette com k=90: {silhouette}")

# COMMAND ----------



# COMMAND ----------

# Obtendo centro dos clusteres
centers = model.clusterCenters()

# Analizando características dos clusteres
cluster_summary = predictions.groupBy("prediction").agg(
    F.avg("col1").alias("avg_col1"),
    F.avg("col1").alias("avg_col2"),
    F.avg("col1").alias("avg_col3"),
    F.avg("col1").alias("avg_col4"),
    F.avg("col1").alias("avg_col5"),
    F.avg("col1").alias("avg_col6"),
    F.avg("col1").alias("avg_col7"),
    F.avg("col1").alias("avg_col8"),
    F.avg("col1").alias("avg_col9"),
    F.avg("col1").alias("avg_col10"),
    F.avg("col1").alias("avg_col11"),
    F.avg("col1").alias("avg_col12"),
    F.avg("col1").alias("avg_col13"),
    F.avg("col1").alias("avg_col14"),
    F.avg("col1").alias("avg_col15")    
)
cluster_summary.show()
print(type(centers))

# COMMAND ----------

display(predictions)

# COMMAND ----------


input = (data.withColumn('features', vector_to_array('features'))
         .select(col('features'))
)
output = model.transform(input)
signature = infer_signature(input, output)

# COMMAND ----------

import mlflow
import mlflow.spark

# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Set the experiment
mlflow.set_experiment("/Users/claudio.takamiya@databricks.com/microseg_model")

# Log the model
with mlflow.start_run() as run:
    mlflow.spark.log_model(model, f"{catalog_name}.customer_gold.microseg_model", signature=signature)
    mlflow.log_param("k", 90)
    mlflow.log_param("featuresCol", "features")
    mlflow.log_param("clusterCenters", centers)
    #mlflow.spark.save_model(model, f"{catalog_name}.customer_gold.microseg_model")

# COMMAND ----------

run_id = run.info.run_id
model_uri = f"runs:/{run_id}/{catalog_name}.customer_gold.microseg_model/"
model_name = f"{catalog_name}.customer_gold.microseg_model"

mlflow.register_model(model_uri, model_name)

