DECLARE catalog_name STRING DEFAULT 'ctakamiya_bundle';
DECLARE date_param DATE DEFAULT '2024-10-01';

USE CATALOG {{catalog_name}};

CREATE OR REPLACE VIEW customer_silver.vw_aux_interesse AS
SELECT msisdn, 
       partition_date, 
       --existing_prediction,
       pos as interesse,
       distance,
       CASE 
           WHEN distance > 0 AND distance < 2.0 THEN 'ALTO'
           WHEN distance >= 2.0 AND distance < 4.0 THEN 'MÉDIO'
           WHEN distance >= 4.0 AND distance < 6.0 THEN 'BAIXO'
           ELSE 'SEM INTERESSE'
       END AS nvl_interesse
FROM customer_silver.tbl_aux_interesse 
LATERAL VIEW POSEXPLODE(distances_to_centers) AS pos, distance
WHERE partition_date = {{date_param}};

CREATE TABLE IF NOT EXISTS customer_gold.tbl_relatorio_interesse (
msisdn STRING,
interesse STRING,
nvl_interesse STRING,
partition_date DATE,
PRIMARY KEY (msisdn)
)
PARTITIONED BY (partition_date);

INSERT INTO customer_gold.tbl_relatorio_interesse (msisdn, interesse, nvl_interesse, partition_date)
SELECT msisdn, 
       interesse, 
       nvl_interesse, 
       partition_date
FROM customer_silver.vw_aux_interesse;