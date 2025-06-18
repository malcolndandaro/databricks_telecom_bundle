-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW product_subscriptions
AS
select 
  nu_tlfn,
  nu_doct,
  id_prdt,
  dt_ini_plno,
  ID_ESTD_LNHA,
  FL_DEBT_AUTM,
  FL_CNTA_ONLN,
  FL_PLNO_TTLR
from
  STREAM(${confs.p_catalog}.${confs.p_schema_customer_silver}.product_subscriptions)
where
  __END_AT is null

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE fact_subscription (
    SK_DIM_DATE INT COMMENT 'Chave Estrangeira da dimensão: DIM_DATE ',
    SK_DIM_CUSTOMER BIGINT COMMENT 'Chave Estrangeira da dimensão: DIM_CUSTOMER',
    SK_DIM_PRODUCT BIGINT COMMENT 'Chave Estrangeira da dimensão: DIM_PRODUCT',
    ID_ESTD_LNHA	SMALLINT COMMENT 'Estado da Linha: 1 Ativa - 0 Inativa',
    FL_DEBT_AUTM	TINYINT COMMENT 'Flag para indicar se o cliente possui debito automatico ativado: 1 Sim - 0 Não',
    FL_CNTA_ONLN	TINYINT COMMENT 'Flag para indicar se o cliente cadastrou recebimento da fatura por email: 1 Sim - 0 Não',
    FL_PLNO_TTLR	TINYINT COMMENT 'Flag para indicar se o cliente é titular da linha: 1 Sim - 0 Não',
    GOLD_TS	TIMESTAMP COMMENT 'Data de Ingestao do Registro para camada Gold',
    CONSTRAINT SK_FACT_SUBSCRIPTION_DIM_DATE_FK FOREIGN KEY (SK_DIM_DATE) REFERENCES ${confs.p_catalog}.${confs.p_schema_misc}.dim_date,
CONSTRAINT SK_FACT_SUBSCRIPTION_DIM_CUSTOMER_FK FOREIGN KEY (SK_DIM_CUSTOMER) REFERENCES ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_customer,
CONSTRAINT SK_FACT_SUBSCRIPTION_DIM_PRODUCT_FK FOREIGN KEY (SK_DIM_PRODUCT) REFERENCES ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_product
    )
CLUSTER BY (SK_DIM_DATE, SK_DIM_CUSTOMER, SK_DIM_PRODUCT)
as
SELECT
  dim_date.SK_DIM_DATE,
  dim_customer.SK_DIM_CUSTOMER,
  dim_product.SK_DIM_PRODUCT,
  ID_ESTD_LNHA,
  FL_DEBT_AUTM,
  FL_CNTA_ONLN,
  FL_PLNO_TTLR,
  now() as GOLD_TS
from
  STREAM(LIVE.product_subscriptions) product_subscriptions
  join ${confs.p_catalog}.${confs.p_schema_misc}.dim_date as dim_date on product_subscriptions.dt_ini_plno = dim_date.DATA
join ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_customer as dim_customer on product_subscriptions.nu_doct = dim_customer.nu_doct
  and product_subscriptions.nu_tlfn = dim_customer.nu_tlfn
  and dim_customer.__end_at is null
  join ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_product as dim_product on product_subscriptions.id_prdt = dim_product.id_prdt
  and dim_product.__end_at is null
  
