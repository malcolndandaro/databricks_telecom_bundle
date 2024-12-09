-- Databricks notebook source
create schema if not exists resource_silver

-- COMMAND ----------

create view if not exists resource_silver.tbl_antena
as
select * from resource_bronze.tbl_antena

-- COMMAND ----------

create view if not exists resource_silver.tbl_navegacao
as
select * from resource_bronze.tbl_navegacao
