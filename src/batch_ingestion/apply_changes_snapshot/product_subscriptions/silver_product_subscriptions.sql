-- Databricks notebook source
CREATE TEMPORARY LIVE VIEW bronze_product_subscriptions
(CONSTRAINT tamanho_telefone EXPECT ( len(nu_tlfn) <= 13)) AS
select * except(_rescued_data, _metadata_file_path, bronze_ts) from ${confs.p_catalog}.customer_bronze.product_subscriptions

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW aux_bronze_product_subscriptions
as
select *, now() as silver_ts from LIVE.bronze_product_subscriptions

-- COMMAND ----------

CREATE STREAMING TABLE product_subscriptions
(
    nu_tlfn STRING NOT NULL COMMENT 'Telefone do Cliente',
    dt_prmr_atvc_lnha DATE NOT NULL COMMENT 'Data de Ativação da Linha',
    dt_dstv_lnha DATE COMMENT 'Data de Cancelamento da Linha',
    nu_doct STRING NOT NULL COMMENT 'Numero do Documento do Cliente',
    id_prdt INT NOT NULL COMMENT 'ID do Produto Contratado',
    ds_prdt STRING COMMENT 'Descrição do Produto Contratado: Pós Pago / Pré Pago',
    ds_plno STRING COMMENT 'Descrição do Plano Contratado',
    id_estd_lnha SMALLINT COMMENT 'Estado da Linha: 1 Ativa - 0 Inativa',
    cd_ddd SMALLINT COMMENT 'DDD do Cliente',
    uf STRING COMMENT 'UF de Cadastro do Cliente',
    no_lgrd STRING COMMENT 'Logradouro',
    no_imovel STRING COMMENT 'Numero do Endereço Residencial',
    no_brro STRING COMMENT 'Bairo',
    nu_cep STRING COMMENT 'CEP',
    no_mnco STRING COMMENT 'Municipio',
    cd_ibge_mnco STRING COMMENT 'Codigo do IBGE do Municipio',
    id_disp_xdsl TINYINT COMMENT 'Flag para indicar se o cliente possui plano XDSL: 1 Sim - 0 Não',
    id_disp_fttc TINYINT COMMENT 'Flag para indicar se o cliente possui plano FTTC: 1 Sim - 0 Não',
    id_disp_ftth TINYINT COMMENT 'Flag para indicar se o cliente possui plano FTTH: 1 Sim - 0 Não',
    fl_plno_dscn TINYINT COMMENT 'Flag para indicar se o cliente possui descontos atrelados ao plano: 1 Sim - 0 Não',
    fl_debt_autm TINYINT COMMENT 'Flag para indicar se o cliente possui debito automatico ativado: 1 Sim - 0 Não',
    fl_cnta_onln TINYINT COMMENT 'Flag para indicar se o cliente cadastrou recebimento da fatura por email: 1 Sim - 0 Não',
    fl_plno_ttlr TINYINT COMMENT 'Flag para indicar se o cliente é titular da linha: 1 Sim - 0 Não',
    nu_imei_aprl STRING COMMENT 'Numero do Imei do Aparelho do Cliente',
    ds_modl_orig_aprl STRING COMMENT 'Descrição do Modelo do Aparelho',
    fl_vivo_total INT COMMENT 'Flag para indicar se o cliente possui plano Vivo Total: 1 Sim - 0 Não',
    dt_trca_aprl DATE COMMENT 'Data da Troca de Aparelho',
    dt_ini_plno DATE COMMENT 'Data de Inicio do Plano atual',
    user_id STRING NOT NULL COMMENT 'UUID Unico do Cliente',
    silver_ts TIMESTAMP COMMENT 'Data de Ingestao do Registro para camada silver',
    __START_AT TIMESTAMP COMMENT 'Data de Inicio do Registro',
    __END_AT TIMESTAMP COMMENT 'Data Fim do Registro',
    CONSTRAINT pk_product_subscription PRIMARY KEY (nu_tlfn, nu_doct, user_id, id_prdt, dt_prmr_atvc_lnha)

)
