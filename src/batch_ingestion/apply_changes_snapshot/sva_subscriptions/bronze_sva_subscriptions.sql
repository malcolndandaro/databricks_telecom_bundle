-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW sva_subscriptions
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
    bronze_ts TIMESTAMP COMMENT 'Data e Hora da Ingestao do Dado',
    _metadata_file_path STRING COMMENT 'Caminho do Arquivo de Origem',
    CONSTRAINT pk_sva_subscriptions PRIMARY KEY (msisdn, productid, data_contratacao)
)
AS SELECT *, current_timestamp as bronze_ts, _metadata.file_path as _metadata_file_path FROM parquet.`/Volumes/${confs.p_catalog}/ingestion/raw_data/customer/sva_subscriptions/`

-- COMMAND ----------


