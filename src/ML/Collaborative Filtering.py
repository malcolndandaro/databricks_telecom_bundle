# Databricks notebook source
dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")
dbutils.widgets.text("date_param","2024-09-01", "Data parâmetro")
catalog_name = dbutils.widgets.get("catalog_name")
date_param = dbutils.widgets.get("date_param")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, explode, lit


# COMMAND ----------

# feature_table_atendimento = spark.read.table(f"{catalog_name}.customer_gold.fs_atendimento")
feature_table_produtos = spark.read.table(f"{catalog_name}.customer_gold.fs_produtos")
product_columns = [col for col in feature_table_produtos.columns if col != 'nu_tlfn']

# COMMAND ----------

display(feature_table_produtos)

# COMMAND ----------



from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy("nu_tlfn")
mapped_df = feature_table_produtos.select("nu_tlfn").distinct().withColumn("seq_id", row_number().over(window_spec))



display(mapped_df)

# COMMAND ----------

# feature_table_atendimento = feature_table_atendimento.withColumn("nu_tlfn", col("msisdn")).drop("msisdn")
# feature_table_atendimento = feature_table_atendimento.join(mapped_df, on="nu_tlfn", how="inner").drop("nu_tlfn").withColumnRenamed("seq_id", "nu_tlfn")

# COMMAND ----------

#display(feature_table_atendimento)

# COMMAND ----------

feature_table_produtos = feature_table_produtos.join(mapped_df, on="nu_tlfn", how="inner").drop("nu_tlfn").withColumnRenamed("seq_id", "nu_tlfn")

display(feature_table_produtos)

# COMMAND ----------

from pyspark.sql.functions import col, explode, create_map
from itertools import chain

melted_df = feature_table_produtos.select(
    col("nu_tlfn").alias("user"),
    explode(
        create_map(
            list(chain(*[(lit(c), col(c)) for c in product_columns]))
        )
    ).alias("item", "feedback")
)

display(melted_df)

# COMMAND ----------

melted_df = melted_df.repartition(1000)

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit, when, monotonically_increasing_id

# Criar IDs inteiros para itens com base em valores conhecidos
item_mapping = {f"col{i}": i for i in range(1, 60)}
mapping_expr = create_map([lit(x) for x in chain(*item_mapping.items())])

melted_df = melted_df.withColumn("item", mapping_expr[col("item")].cast("integer"))

display(melted_df)

# COMMAND ----------

# Filtrar linhas onde o feedback é 0, pois o ALS funciona melhor com apenas feedback positivo
positive_df = melted_df.filter(col("feedback") == 1)

# Adicionar uma coluna constante para 'rating' (ALS requer esta coluna)
positive_df = positive_df.withColumn("rating", lit(1.0))

# Dividir os dados em conjuntos de treinamento e teste
(training, test) = positive_df.randomSplit([0.8, 0.2])

# COMMAND ----------

display(training)

# COMMAND ----------


# Construir o modelo de recomendação usando ALS nos dados de treinamento
als = ALS(
    maxIter=10,
    regParam=0.01,
    userCol="user",
    itemCol="item",
    ratingCol="rating",
    coldStartStrategy="drop",
    implicitPrefs=True  # Definir como True para feedback implícito
)


# COMMAND ----------

# Fit the model
model = als.fit(training)

# Make predictions on the test set
predictions = model.transform(test)

# Cast the prediction column to double type
predictions = predictions.withColumn("prediction", predictions["prediction"].cast("double"))

# Evaluate the model using AUC as the metric
evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="prediction", 
    labelCol="rating", 
    metricName="areaUnderROC"
)
auc = evaluator.evaluate(predictions)
print(f"AUC = {auc}")

# Generate the top 10 product recommendations for all users
user_recs = model.recommendForAllUsers(10)

# Display the recommendations
display(user_recs)

# COMMAND ----------

# Mostrar recomendações
user_recs.show(truncate=False)


# COMMAND ----------





# Se você quiser obter recomendações para um usuário específico (nr_tlfn)
specific_user = 12345  # Substitua por um nr_tlfn real
user_rec = model.recommendForUserSubset(spark.createDataFrame([(specific_user,)], ["user"]), 10)
user_rec.show(truncate=False)


