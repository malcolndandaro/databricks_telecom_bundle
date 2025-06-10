-- Databricks notebook source
CREATE TEMPORARY LIVE VIEW bronze_sva_subscriptions
(
  CONSTRAINT tamanho_telefone EXPECT ( len(msisdn) <= 13),
  CONSTRAINT tipo_linha EXPECT (tplinha in ('Fixa', 'Móvel'))
) AS
select * except(_metadata_file_path, bronze_ts)
from ${confs.p_catalog}.customer_bronze.sva_subscriptions

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW aux_bronze_sva_subscriptions
AS
select *, now() as silver_ts from LIVE.bronze_sva_subscriptions

-- COMMAND ----------

CREATE STREAMING TABLE sva_subscriptions
(
    msisdn STRING  NOT NULL COMMENT 'Telefone do Cliente',
    productid STRING NOT NULL COMMENT 'Identificador do Produto',
    productname STRING COMMENT 'Nome do Produto',
    protocol_number STRING COMMENT 'Numero do Protocolo Gerado na Contratação',
    spname STRING COMMENT 'Provedor do Serviço',
    subscribestate STRING COMMENT 'Status do Seriviço: Ativo, Cancelado, Suspenso',
    productaccountinggroup STRING COMMENT 'Informação do Grupo do Produto',
    client STRING COMMENT 'Segmento do Cliente',
    servicetype STRING COMMENT 'Tipo do Serviço: Bundle / Avulso',
    tplinha STRING COMMENT 'Tipo da Linha do Cliente: Fixa / Movel',
    grossvalue INT COMMENT 'Valor Bruto do Serviço',
    company STRING COMMENT 'Empesa',
    taxpis DECIMAL(7,2) COMMENT 'Percentual de PIS',
    taxcofins DECIMAL(7,2) COMMENT 'Percentual de Cofins',
    taxiss DECIMAL(7,2) COMMENT 'Percentual de ISS',
    discountvalue STRING COMMENT 'Valor de Desconto Atrelado ao Serviço',
    data_contratacao STRING NOT NULL COMMENT 'Data e Hora da Contratação do Serviço',
    silver_ts TIMESTAMP COMMENT 'Data de Ingestao do Registro para camada silver',
    __START_AT TIMESTAMP COMMENT 'Data de Inicio do Registro',
    __END_AT TIMESTAMP COMMENT 'Data Fim do Registro',
    CONSTRAINT pk_sva_subscriptions PRIMARY KEY (msisdn, productid, data_contratacao)

)
