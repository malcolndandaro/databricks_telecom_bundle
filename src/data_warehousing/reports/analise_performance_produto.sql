-- Databricks notebook source
CREATE OR REPLACE MATERIALIZED VIEW analise_performance_produto
AS
SELECT
    case when c.CLIENT = 'P' then 'Purpura'
          when c.CLIENT = 'S' then 'Silver'
          when c.CLIENT = 'G' then 'Gold'
          when c.CLIENT = 'PL' then 'Platinum'
          when C.CLIENT = 'V' then 'VIP'
          end categoria_cliente,
    c.nu_tlfn,
    d.ANO,
    d.TRIMESTRE,
    p.DS_PRDT,
    p.DS_PLNO,
    COUNT(DISTINCT f.SK_DIM_CUSTOMER) AS CUSTOMER_COUNT,
    SUM(f.VL_FTRA) AS TOTAL_REVENUE,
    AVG(f.VL_FTRA) AS AVG_INVOICE_VALUE
FROM ${confs.p_catalog}.${confs.p_schema_billing_gold}.fact_invoicing f
JOIN ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_product p ON f.SK_DIM_PRODUCT = p.SK_DIM_PRODUCT
JOIN ${confs.p_catalog}.${confs.p_schema_misc}.dim_date d ON f.SK_DIM_DATE = d.SK_DIM_DATE
join ${confs.p_catalog}.${confs.p_schema_customer_gold}.dim_customer c on f.SK_DIM_CUSTOMER = c.SK_DIM_CUSTOMER
GROUP BY ALL
ORDER BY ANO, TRIMESTRE DESC
;
