# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "misc")
dbutils.widgets.text("p_table", "aux_tbl_produtos")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG $p_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists $p_schema.aux_tbl_produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $p_schema.aux_tbl_produtos AS
# MAGIC SELECT * FROM (
# MAGIC   VALUES
# MAGIC     (1, 'Pós Pago', 'Plano Família 50GB'),
# MAGIC     (2, 'Pré Pago', 'Recarga Fácil 20'),
# MAGIC     (3, 'Pós Pago', 'Plano Empresarial 100GB'),
# MAGIC     (4, 'Pós Pago', 'Plano Jovem 30GB'),
# MAGIC     (5, 'Pré Pago', 'Cartão Pré-pago 30 Dias'),
# MAGIC     (6, 'Pós Pago', 'Plano Controle 20GB'),
# MAGIC     (7, 'Pós Pago', 'Plano Premium Ilimitado'),
# MAGIC     (8, 'Pré Pago', 'Recarga Mensal 5GB'),
# MAGIC     (9, 'Pós Pago', 'Plano Família 100GB'),
# MAGIC     (10, 'Pós Pago', 'Plano Executivo 80GB'),
# MAGIC     (11, 'Pré Pago', 'Cartão Pré-pago 15 Dias'),
# MAGIC     (12, 'Pós Pago', 'Plano Estudante 40GB'),
# MAGIC     (13, 'Pós Pago', 'Plano Controle 40GB'),
# MAGIC     (14, 'Pré Pago', 'Recarga Semanal 2GB'),
# MAGIC     (15, 'Pós Pago', 'Plano Empresarial 200GB'),
# MAGIC     (16, 'Pós Pago', 'Plano Família 150GB'),
# MAGIC     (17, 'Pré Pago', 'Cartão Pré-pago 60 Dias'),
# MAGIC     (18, 'Pós Pago', 'Plano Jovem 50GB'),
# MAGIC     (19, 'Pós Pago', 'Plano Premium 5G'),
# MAGIC     (20, 'Pré Pago', 'Recarga Mensal 10GB'),
# MAGIC     (21, 'Pós Pago', 'Plano Controle 60GB'),
# MAGIC     (22, 'Pós Pago', 'Plano Executivo 120GB'),
# MAGIC     (23, 'Pré Pago', 'Cartão Pré-pago 45 Dias'),
# MAGIC     (24, 'Pós Pago', 'Plano Estudante 60GB'),
# MAGIC     (25, 'Pós Pago', 'Plano Família 200GB'),
# MAGIC     (26, 'Pré Pago', 'Recarga Quinzenal 3GB'),
# MAGIC     (27, 'Pós Pago', 'Plano Empresarial 300GB'),
# MAGIC     (28, 'Pós Pago', 'Plano Controle 80GB'),
# MAGIC     (29, 'Pré Pago', 'Cartão Pré-pago 90 Dias'),
# MAGIC     (30, 'Pós Pago', 'Plano Premium 5G Ilimitado')
# MAGIC ) AS products(id_prdt, ds_prdt, ds_plno);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $p_schema.aux_tbl_produtos AS
# MAGIC SELECT *, 
# MAGIC   CASE 
# MAGIC     WHEN ds_prdt = 'Pós Pago' THEN
# MAGIC       CASE
# MAGIC         WHEN ds_plno LIKE '%Família%' THEN 
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%50GB%' THEN 159.99
# MAGIC             WHEN ds_plno LIKE '%100GB%' THEN 199.99
# MAGIC             WHEN ds_plno LIKE '%150GB%' THEN 239.99
# MAGIC             WHEN ds_plno LIKE '%200GB%' THEN 279.99
# MAGIC             ELSE 199.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Empresarial%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%100GB%' THEN 249.99
# MAGIC             WHEN ds_plno LIKE '%200GB%' THEN 349.99
# MAGIC             WHEN ds_plno LIKE '%300GB%' THEN 449.99
# MAGIC             ELSE 299.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Jovem%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%30GB%' THEN 79.99
# MAGIC             WHEN ds_plno LIKE '%50GB%' THEN 99.99
# MAGIC             ELSE 89.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Controle%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%20GB%' THEN 59.99
# MAGIC             WHEN ds_plno LIKE '%40GB%' THEN 79.99
# MAGIC             WHEN ds_plno LIKE '%60GB%' THEN 99.99
# MAGIC             WHEN ds_plno LIKE '%80GB%' THEN 119.99
# MAGIC             ELSE 89.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Premium%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%Ilimitado%' THEN 199.99
# MAGIC             WHEN ds_plno LIKE '%5G%' THEN 249.99
# MAGIC             ELSE 179.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Executivo%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%80GB%' THEN 179.99
# MAGIC             WHEN ds_plno LIKE '%120GB%' THEN 219.99
# MAGIC             ELSE 199.99
# MAGIC           END
# MAGIC         WHEN ds_plno LIKE '%Estudante%' THEN
# MAGIC           CASE
# MAGIC             WHEN ds_plno LIKE '%40GB%' THEN 69.99
# MAGIC             WHEN ds_plno LIKE '%60GB%' THEN 89.99
# MAGIC             ELSE 79.99
# MAGIC           END
# MAGIC         ELSE 99.99
# MAGIC       END
# MAGIC     WHEN ds_prdt = 'Pré Pago' THEN
# MAGIC       CASE
# MAGIC         WHEN ds_plno LIKE '%Recarga Fácil%' THEN 20.00
# MAGIC         WHEN ds_plno LIKE '%Cartão Pré-pago 30 Dias%' THEN 30.00
# MAGIC         WHEN ds_plno LIKE '%Recarga Mensal 5GB%' THEN 25.00
# MAGIC         WHEN ds_plno LIKE '%Cartão Pré-pago 15 Dias%' THEN 15.00
# MAGIC         WHEN ds_plno LIKE '%Recarga Semanal 2GB%' THEN 10.00
# MAGIC         WHEN ds_plno LIKE '%Cartão Pré-pago 60 Dias%' THEN 60.00
# MAGIC         WHEN ds_plno LIKE '%Recarga Mensal 10GB%' THEN 40.00
# MAGIC         WHEN ds_plno LIKE '%Cartão Pré-pago 45 Dias%' THEN 45.00
# MAGIC         WHEN ds_plno LIKE '%Recarga Quinzenal 3GB%' THEN 20.00
# MAGIC         WHEN ds_plno LIKE '%Cartão Pré-pago 90 Dias%' THEN 90.00
# MAGIC         ELSE 30.00
# MAGIC       END
# MAGIC     ELSE 0.00
# MAGIC   END AS gross
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC     (1, 'Pós Pago', 'Plano Família 50GB'),
# MAGIC     (2, 'Pré Pago', 'Recarga Fácil 20'),
# MAGIC     (3, 'Pós Pago', 'Plano Empresarial 100GB'),
# MAGIC     (4, 'Pós Pago', 'Plano Jovem 30GB'),
# MAGIC     (5, 'Pré Pago', 'Cartão Pré-pago 30 Dias'),
# MAGIC     (6, 'Pós Pago', 'Plano Controle 20GB'),
# MAGIC     (7, 'Pós Pago', 'Plano Premium Ilimitado'),
# MAGIC     (8, 'Pré Pago', 'Recarga Mensal 5GB'),
# MAGIC     (9, 'Pós Pago', 'Plano Família 100GB'),
# MAGIC     (10, 'Pós Pago', 'Plano Executivo 80GB'),
# MAGIC     (11, 'Pré Pago', 'Cartão Pré-pago 15 Dias'),
# MAGIC     (12, 'Pós Pago', 'Plano Estudante 40GB'),
# MAGIC     (13, 'Pós Pago', 'Plano Controle 40GB'),
# MAGIC     (14, 'Pré Pago', 'Recarga Semanal 2GB'),
# MAGIC     (15, 'Pós Pago', 'Plano Empresarial 200GB'),
# MAGIC     (16, 'Pós Pago', 'Plano Família 150GB'),
# MAGIC     (17, 'Pré Pago', 'Cartão Pré-pago 60 Dias'),
# MAGIC     (18, 'Pós Pago', 'Plano Jovem 50GB'),
# MAGIC     (19, 'Pós Pago', 'Plano Premium 5G'),
# MAGIC     (20, 'Pré Pago', 'Recarga Mensal 10GB'),
# MAGIC     (21, 'Pós Pago', 'Plano Controle 60GB'),
# MAGIC     (22, 'Pós Pago', 'Plano Executivo 120GB'),
# MAGIC     (23, 'Pré Pago', 'Cartão Pré-pago 45 Dias'),
# MAGIC     (24, 'Pós Pago', 'Plano Estudante 60GB'),
# MAGIC     (25, 'Pós Pago', 'Plano Família 200GB'),
# MAGIC     (26, 'Pré Pago', 'Recarga Quinzenal 3GB'),
# MAGIC     (27, 'Pós Pago', 'Plano Empresarial 300GB'),
# MAGIC     (28, 'Pós Pago', 'Plano Controle 80GB'),
# MAGIC     (29, 'Pré Pago', 'Cartão Pré-pago 90 Dias'),
# MAGIC     (30, 'Pós Pago', 'Plano Premium 5G Ilimitado')
# MAGIC ) AS products(id_prdt, ds_prdt, ds_plno)
