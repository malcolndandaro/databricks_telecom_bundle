-- Databricks notebook source
SET confs.p_catalog = dev

-- COMMAND ----------


CREATE TEMPORARY STREAMING LIVE VIEW aux_silver_invoicing 
AS
select
  *
from
  STREAM(${confs.p_catalog}.${confs.p_schema_billing_bronze}.invoicing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE invoicing
(
    nu_doct STRING COMMENT 'Numero do Documento do Cliente',
    ds_prdt STRING COMMENT 'Descrição do Produto Contratado: Pré Pago / Pós Pago',
    ds_plno STRING COMMENT 'Descrição do Plano Contratado',
    cd_ddd SMALLINT COMMENT 'DDD do Cliente',
    uf STRING COMMENT 'UF de Cadastro do Cliente',
    no_lgrd STRING COMMENT 'Logradouro',
    no_imovel STRING COMMENT 'Numero do Endereço Residencial',
    no_brro STRING COMMENT 'Bairo',
    nu_cep STRING COMMENT 'CEP',
    no_mnco STRING COMMENT 'Municipio',
    cd_ibge_mnco STRING COMMENT 'Codigo do IBGE do Municipio',
    dt_ciclo_rcrg DATE COMMENT 'Data do Ciclo da Fatura Pós / Data da Recarga Pré',
    dt_vncmt TINYINT COMMENT 'Data de Vencimento da Fatura',
    vl_ftra DECIMAL(13,2) COMMENT 'Valor da Fatura do Cliente',
    vl_rcrg DECIMAL(5,2) COMMENT 'Valor da Recarga do Cliente (Apenas Pré Pago)',
    ds_cnl_rcrg STRING COMMENT 'Canal de Recarga: BANCARIO, ELETRONICO',
    dt_pgto DATE COMMENT 'Data de Pagamento',
    user_id STRING COMMENT 'UUID Unico do Cliente',
    CONSTRAINT pk_invoicing PRIMARY KEY (nu_doct, dt_ciclo_rcrg)
)

-- COMMAND ----------

APPLY CHANGES INTO
  LIVE.invoicing
FROM
  stream(LIVE.aux_silver_invoicing)
KEYS
  (nu_doct, dt_ciclo_rcrg)
SEQUENCE BY
  _metadata_file_modification_time
COLUMNS * EXCEPT
  (_rescued_data, _metadata_file_path, bronze_ts, _metadata_file_modification_time)
STORED AS
  SCD TYPE 1;
