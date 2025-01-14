USE CATALOG {{catalog_name}};

CREATE OR REPLACE TEMPORARY VIEW nav_cat_temp AS
SELECT 
  nr_tlfn,
  cd_cgi,  
  ds_pctl,
    CASE WHEN ds_pctl = '1' THEN 1 ELSE 0 END AS col1,
    CASE WHEN ds_pctl = '2' THEN 1 ELSE 0 END AS col2,
    CASE WHEN ds_pctl = '3' THEN 1 ELSE 0 END AS col3,
    CASE WHEN ds_pctl = '4' THEN 1 ELSE 0 END AS col4,
    CASE WHEN ds_pctl = '5' THEN 1 ELSE 0 END AS col5,
    CASE WHEN ds_pctl = '6' THEN 1 ELSE 0 END AS col6,
    CASE WHEN ds_pctl = '7' THEN 1 ELSE 0 END AS col7,
    CASE WHEN ds_pctl = '8' THEN 1 ELSE 0 END AS col8,
    CASE WHEN ds_pctl = '9' THEN 1 ELSE 0 END AS col9,
    CASE WHEN ds_pctl = '10' THEN 1 ELSE 0 END AS col10,
    CASE WHEN ds_pctl = '11' THEN 1 ELSE 0 END AS col11,
    CASE WHEN ds_pctl = '12' THEN 1 ELSE 0 END AS col12,
    CASE WHEN ds_pctl = '13' THEN 1 ELSE 0 END AS col13,
    CASE WHEN ds_pctl = '14' THEN 1 ELSE 0 END AS col14,
    CASE WHEN ds_pctl = '15' OR NOT ds_pctl RLIKE '^[0-9]+$' THEN 1 ELSE 0 END AS col15,
    partition_date
FROM resource_bronze.tbl_navegacao
WHERE partition_date = {{data_param}};

CREATE OR REPLACE TEMPORARY VIEW nav_cat_temp_sum AS
  SELECT nr_tlfn, cd_cgi,
         SUM(col1)  / count(cd_cgi) as cat1,
         SUM(col2)  / count(cd_cgi) as cat2,
         SUM(col3)  / count(cd_cgi) as cat3,
         SUM(col4)  / count(cd_cgi) as cat4,
         SUM(col5)  / count(cd_cgi) as cat5,
         SUM(col6)  / count(cd_cgi) as cat6,
         SUM(col7)  / count(cd_cgi) as cat7,
         SUM(col8)  / count(cd_cgi) as cat8,
         SUM(col9)  / count(cd_cgi) as cat9,
         SUM(col10) / count(cd_cgi) as cat10,
         SUM(col11) / count(cd_cgi) as cat11,
         SUM(col12) / count(cd_cgi) as cat12,
         SUM(col13) / count(cd_cgi) as cat13,
         SUM(col14) / count(cd_cgi) as cat14,
         SUM(col15) / count(cd_cgi) as cat15,
         partition_date
  FROM nav_cat_temp
  GROUP BY nr_tlfn, cd_cgi, partition_date
ORDER BY nr_tlfn, cd_cgi, partition_date;


CREATE OR REPLACE TEMPORARY VIEW view_fs_interesse AS
    SELECT n.nr_tlfn, n.cd_cgi, e.lat_decimal, e.lon_decimal, 
           n.cat1, n.cat2, n.cat3, n.cat4, n.cat5, n.cat6, n.cat7, 
           n.cat8, n.cat9, n.cat10, n.cat11, n.cat12, n.cat13, 
           n.cat14, n.cat15, n.partition_date
    FROM  
      nav_cat_temp_sum n
    JOIN   
      resource_bronze.erb_coord_dec e
    ON 
      n.cd_cgi = e.NumEstacao
    WHERE n.partition_date = {{data_param}};


CREATE TABLE IF NOT EXISTS customer_gold.fs_interesse (
nr_tlfn STRING,
lat_decimal double,
lon_decimal double,
cd_cgi bigint,
col1 double,
col2 double,
col3 double,
col4 double,
col5 double,
col6 double,
col7 double,
col8 double,
col9 double,
col10 double,
col11 double,
col12 double,
col13 double,
col14 double,
col15 double,
partition_date DATE,
PRIMARY KEY (nr_tlfn, cd_cgi, partition_date)
);


MERGE INTO customer_gold.fs_interesse AS target
USING view_fs_interesse AS source
ON target.nr_tlfn = source.nr_tlfn 
    AND target.partition_date = source.partition_date 
    AND target.cd_cgi = source.cd_cgi
WHEN MATCHED THEN
  UPDATE SET
    lat_decimal = source.lat_decimal,
    lon_decimal = source.lon_decimal,
    cd_cgi = source.cd_cgi,
    col1 = source.cat1,
    col2 = source.cat2,
    col3 = source.cat3,
    col4 = source.cat4,
    col5 = source.cat5,
    col6 = source.cat6,
    col7 = source.cat7,
    col8 = source.cat8,
    col9 = source.cat9,
    col10 = source.cat10,
    col11 = source.cat11,
    col12 = source.cat12,
    col13 = source.cat13,
    col14 = source.cat14,
    col15 = source.cat15    
WHEN NOT MATCHED THEN
  INSERT (nr_tlfn, lat_decimal, lon_decimal, 
          cd_cgi, col1, col2, col3, col4, 
          col5, col6, col7, col8, col9, 
          col10, col11, col12, col13, 
          col14, col15, partition_date)
  VALUES (source.nr_tlfn, source.lat_decimal, 
          source.lon_decimal, source.cd_cgi, 
          source.cat1, source.cat2, source.cat3, 
          source.cat4, source.cat5, source.cat6, 
          source.cat7, source.cat8, source.cat9, 
          source.cat10, source.cat11, source.cat12, 
          source.cat13, source.cat14, source.cat15, 
          source.partition_date);
