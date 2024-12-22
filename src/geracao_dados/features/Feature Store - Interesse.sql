USE CATALOG {{catalog_name}};


CREATE SCHEMA IF NOT EXISTS customer_gold;

CREATE OR REPLACE TEMPORARY VIEW nr_tlfn_cat AS
SELECT DISTINCT
  n.nr_tlfn,
  n.cd_cgi,
  n.ds_pctl,
  e.lat_decimal,
  e.lon_decimal
FROM
  resource_bronze.tbl_navegacao n
JOIN
  resource_bronze.erb_coord_dec e
ON
  n.cd_cgi = e.NumEstacao
WHERE
  n.partition_date = {{data_param}};



CREATE OR REPLACE TEMPORARY VIEW temp_pivoted AS
SELECT 
  nr_tlfn,
  lat_decimal,
  lon_decimal,
  cd_cgi,
  MAX(CASE WHEN ds_pctl = '1' THEN 1 ELSE 0 END) AS col1,
  MAX(CASE WHEN ds_pctl = '2' THEN 1 ELSE 0 END) AS col2,
  MAX(CASE WHEN ds_pctl = '3' THEN 1 ELSE 0 END) AS col3,
  MAX(CASE WHEN ds_pctl = '4' THEN 1 ELSE 0 END) AS col4,
  MAX(CASE WHEN ds_pctl = '5' THEN 1 ELSE 0 END) AS col5,
  MAX(CASE WHEN ds_pctl = '6' THEN 1 ELSE 0 END) AS col6,
  MAX(CASE WHEN ds_pctl = '7' THEN 1 ELSE 0 END) AS col7,
  MAX(CASE WHEN ds_pctl = '8' THEN 1 ELSE 0 END) AS col8,
  MAX(CASE WHEN ds_pctl = '9' THEN 1 ELSE 0 END) AS col9,
  MAX(CASE WHEN ds_pctl = '10' THEN 1 ELSE 0 END) AS col10,
  MAX(CASE WHEN ds_pctl = '11' THEN 1 ELSE 0 END) AS col11,
  MAX(CASE WHEN ds_pctl = '12' THEN 1 ELSE 0 END) AS col12,
  MAX(CASE WHEN ds_pctl = '13' THEN 1 ELSE 0 END) AS col13,
  MAX(CASE WHEN ds_pctl = '14' THEN 1 ELSE 0 END) AS col14,
  MAX(CASE WHEN ds_pctl = '15' OR NOT ds_pctl RLIKE '^[0-9]+$' THEN 1 ELSE 0 END) AS col15,
  date({{data_param}}) as partition_date
FROM nr_tlfn_cat
GROUP BY 
  nr_tlfn,
  lat_decimal,
  lon_decimal,
  cd_cgi,
  date({{data_param}});


CREATE OR REPLACE TEMPORARY VIEW temp_pivoted_dedup AS
SELECT *
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY nr_tlfn, cd_cgi, partition_date ORDER BY lat_decimal, lon_decimal) as rn
  FROM temp_pivoted
)
WHERE rn = 1;



CREATE TABLE IF NOT EXISTS customer_gold.fs_interesse (
nr_tlfn STRING,
lat_decimal double,
lon_decimal double,
cd_cgi bigint,
col1 int,
col2 int,
col3 int,
col4 int,
col5 int,
col6 int,
col7 int,
col8 int,
col9 int,
col10 int,
col11 int,
col12 int,
col13 int,
col14 int,
col15 int,
partition_date DATE,
PRIMARY KEY (nr_tlfn, cd_cgi, partition_date)
)
PARTITIONED BY (partition_date);




MERGE INTO  customer_gold.fs_interesse AS target
USING temp_pivoted_dedup AS source
ON target.nr_tlfn = source.nr_tlfn 
    AND target.partition_date = source.partition_date 
    AND target.cd_cgi = source.cd_cgi
WHEN MATCHED THEN
  UPDATE SET
    lat_decimal = source.lat_decimal,
    lon_decimal = source.lon_decimal,
    cd_cgi = source.cd_cgi,
    col1 = source.col1,
    col2 = source.col2,
    col3 = source.col3,
    col4 = source.col4,
    col5 = source.col5,
    col6 = source.col6,
    col7 = source.col7,
    col8 = source.col8,
    col9 = source.col9,
    col10 = source.col10,
    col11 = source.col11,
    col12 = source.col12,
    col13 = source.col13,
    col14 = source.col14,
    col15 = source.col15    
WHEN NOT MATCHED THEN
  INSERT (nr_tlfn, lat_decimal, lon_decimal, 
          cd_cgi, col1, col2, col3, col4, 
          col5, col6, col7, col8, col9, 
          col10, col11, col12, col13, 
          col14, col15, partition_date)
  VALUES (source.nr_tlfn, source.lat_decimal, 
          source.lon_decimal, source.cd_cgi, 
          source.col1, source.col2, source.col3, 
          source.col4, source.col5, source.col6, 
          source.col7, source.col8, source.col9, 
          source.col10, source.col11, source.col12, 
          source.col13, source.col14, source.col15, 
          source.partition_date);