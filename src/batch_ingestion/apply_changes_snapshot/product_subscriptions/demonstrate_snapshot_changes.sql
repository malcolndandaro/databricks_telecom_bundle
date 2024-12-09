-- Databricks notebook source
CREATE WIDGET TEXT p_catalog DEFAULT "dev"

-- COMMAND ----------

with rows_changed as (
  select
    nu_tlfn,
    dt_prmr_atvc_lnha,
    nu_doct,
    id_prdt,
    user_id,
    count(*)
  from
     IDENTIFIER(:p_catalog || '.' || 'customer_silver' || '.' || 'product_subscriptions')
  group by
    all
  having
    count(*) = 2
)
select
  *
from
  IDENTIFIER(:p_catalog || '.' || 'customer_silver' || '.' || 'product_subscriptions') as silver
  join rows_changed on silver.nu_tlfn = rows_changed.nu_tlfn
  and silver.dt_prmr_atvc_lnha = rows_changed.dt_prmr_atvc_lnha
  and silver.nu_doct = rows_changed.nu_doct
  and silver.id_prdt = rows_changed.id_prdt
  and silver.user_id = rows_changed.user_id
order by
  silver.nu_tlfn,
  silver.dt_prmr_atvc_lnha,
  silver.nu_doct,
  silver.id_prdt,
  silver.user_id,
  __START_AT
