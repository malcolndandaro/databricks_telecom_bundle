-- Databricks notebook source
-- Schema creation moved to bundle definition

-- COMMAND ----------

create view if not exists ${catalog_name}.${schema_resource_silver}.tbl_antena
as
select * from ${catalog_name}.${schema_resource_bronze}.tbl_antena

-- COMMAND ----------

create view if not exists ${catalog_name}.${schema_resource_silver}.tbl_navegacao
as
select * from ${catalog_name}.${schema_resource_bronze}.tbl_navegacao
