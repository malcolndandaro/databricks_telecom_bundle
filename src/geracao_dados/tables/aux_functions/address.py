# Databricks notebook source
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_schema", "misc")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog $p_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC $p_schema.lookup_uf_by_ddd(p_codigo_ddd int COMMENT 'Código DDD') 
# MAGIC    RETURNS STRING
# MAGIC    READS SQL DATA SQL SECURITY DEFINER
# MAGIC    NOT DETERMINISTIC
# MAGIC    COMMENT 'Retorna Código do UF com base no DDD'
# MAGIC    RETURN SELECT codigo_uf FROM misc.aux_tbl_municipios WHERE ddd = p_codigo_ddd order by rand() limit 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC $p_schema.lookup_random_city_by_ddd_uf(p_codigo_ddd int COMMENT 'Código DDD', p_codigo_uf string COMMENT 'Código UF') 
# MAGIC    RETURNS INT
# MAGIC    READS SQL DATA SQL SECURITY DEFINER
# MAGIC    NOT DETERMINISTIC
# MAGIC    COMMENT 'Retorna uma cidade aleatoria (CODIGO_IBGE) com base no DDD e UF'
# MAGIC    RETURN SELECT CODIGO_IBGE FROM misc.aux_tbl_municipios WHERE codigo_uf = p_codigo_uf and p_codigo_ddd = ddd order by rand() limit 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC $p_schema.lookup_city_by_ibge(p_codigo_ibge int COMMENT 'an ibg codigo') 
# MAGIC    RETURNS STRING
# MAGIC    READS SQL DATA SQL SECURITY DEFINER
# MAGIC    NOT DETERMINISTIC
# MAGIC    RETURN SELECT nome FROM misc.aux_tbl_municipios WHERE codigo_ibge = p_codigo_ibge limit 1;
# MAGIC
