-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW invoicing
AS
select
  nu_doct,
  ds_plno,
  ds_prdt,
  dt_ciclo_rcrg,
  dt_vncmt,
  dt_pgto,
  vl_ftra,
  vl_rcrg
from
  STREAM(${confs.p_catalog}.billing_silver.invoicing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE fact_invoicing (
    SK_DIM_DATE INT COMMENT 'Chave Estrangeira da dimensão: DIM_DATE ',
    SK_DIM_CUSTOMER BIGINT COMMENT 'Chave Estrangeira da dimensão: DIM_CUSTOMER',
    SK_DIM_PRODUCT BIGINT COMMENT 'Chave Estrangeira da dimensão: DIM_PRODUCT',
    DT_VNCMT TINYINT COMMENT 'Dia do mes de Vencimento da Fatura',
    DT_PGTO DATE COMMENT 'Data do Pagamento da Fatura',
    VL_FTRA DECIMAL(13,2) COMMENT 'Valor da Fatura',
    VL_RCRG DECIMAL(5,2) COMMENT 'Valor do Recarga',
    GOLD_TS	TIMESTAMP COMMENT 'Data de ingestão do Registro para camada Gold',
    CONSTRAINT SK_FACT_INVOICING_DIM_DATE_FK FOREIGN KEY (SK_DIM_DATE) REFERENCES ${confs.p_catalog}.misc.dim_date,
    CONSTRAINT SK_FACT_INVOICING_DIM_CUSTOMER_FK FOREIGN KEY (SK_DIM_CUSTOMER) REFERENCES ${confs.p_catalog}.customer_gold.dim_customer,
    CONSTRAINT SK_FACT_INVOICING_DIM_PRODUCT_FK FOREIGN KEY (SK_DIM_PRODUCT) REFERENCES ${confs.p_catalog}.customer_gold.dim_product
    )
  CLUSTER BY (SK_DIM_DATE, SK_DIM_CUSTOMER, SK_DIM_PRODUCT)
as
select
  dim_date.SK_DIM_DATE,
  dim_customer.SK_DIM_CUSTOMER,
  dim_product.SK_DIM_PRODUCT,
  invoicing.DT_VNCMT,
  invoicing.DT_PGTO,
  invoicing.VL_FTRA,
  invoicing.VL_RCRG,
  now() as GOLD_TS
from
  STREAM(LIVE.invoicing) invoicing
  join ${confs.p_catalog}.misc.dim_date as dim_date on invoicing.dt_ciclo_rcrg = dim_date.DATA
  join ${confs.p_catalog}.customer_gold.dim_customer as dim_customer on invoicing.nu_doct = dim_customer.nu_doct
  and dim_customer.__end_at is null
  join ${confs.p_catalog}.customer_gold.dim_product as dim_product on invoicing.ds_plno = dim_product.ds_plno
  and invoicing.ds_prdt = dim_product.ds_prdt
  and dim_product.__end_at is null
  
