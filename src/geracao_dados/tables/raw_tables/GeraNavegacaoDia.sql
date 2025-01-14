-- use catalog {{catalog_name}};

-- Create the table if it does not exist MODIFICAR FINAL
CREATE TABLE IF NOT EXISTS  identifier(:catalog_name || '.resource_bronze.tbl_navegacao') (
    nr_tlfn	string,
    ds_ip	string,
    cd_imei	string,
    cd_cgi	int,
    dt_ini_nvgc	long,
    dt_fim_nvgc	long,
    ds_host	string,
    ds_pctl	string,
    ds_sbpt	string,
    qtdd_byte_tfgd	int,
    partition_date DATE
)
PARTITIONED BY (partition_date);


DELETE FROM identifier(:catalog_name || '.resource_bronze.tbl_navegacao') WHERE partition_date = :date_param;

-- Insert into the table from the temporary view
INSERT INTO identifier(:catalog_name || '.resource_bronze.tbl_navegacao')
SELECT
    nr_tlfn,
    CONCAT(
        SPLIT(ds_ip, '\\.')[0], '.', 
        SPLIT(ds_ip, '\\.')[1], '.', 
        SPLIT(ds_ip, '\\.')[2], '.', 
        day(to_date(:date_param))
    ) AS ds_ip,
    cd_imei,
    cd_cgi,    
    unix_timestamp(make_timestamp(
        year(to_date(:date_param)), 
        month(to_date(:date_param)), 
        day(to_date(:date_param)), 
        hour(cast(dt_ini_nvgc as timestamp)), 
        minute(cast(dt_ini_nvgc as timestamp)), 
        second(cast(dt_ini_nvgc as timestamp)))) + cast((rand(1) * 50) as int)  AS dt_ini_nvgc, 
    unix_timestamp(make_timestamp(
        year(to_date(:date_param)), 
        month(to_date(:date_param)), 
        day(to_date(:date_param)), 
        hour(cast(dt_fim_nvgc as timestamp)), 
        minute(cast(dt_fim_nvgc as timestamp)), 
        second(cast(dt_fim_nvgc as timestamp)))) + cast((rand(1) * 50) as int)  AS dt_fim_nvgc, 
    ds_host,
    ds_pctl,
    ds_sbpt,
    qtdd_byte_tfgd + cast(rand() * 100 as int) AS qtdd_byte_tfgd,    
    to_date(:date_param) AS partition_date
FROM identifier(:catalog_name || '.misc.aux_tbl_navegacao_pattern');