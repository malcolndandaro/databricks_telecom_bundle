--USE CATALOG identifier( :catalog_name) ;

-- Create the table if it does not exist
CREATE TABLE IF NOT EXISTS identifier(:catalog_name || '.' || 'resource_bronze.tbl_antena') ( -- Mudando para {catalog_name}.resource_bronze
  msisdn STRING,
  tx_uplink LONG,
  tx_downlink LONG,
  qt_volume LONG,
  qt_duration LONG,
  dt_start_time TIMESTAMP,
  dt_end_time TIMESTAMP,
  nu_served_imei STRING,
  cd_cgi STRING,
  cd_pais STRING,
  cd_area STRING,
  partition_date DATE
)
PARTITIONED BY (partition_date);

-- Insert into the table from the temporary view
INSERT INTO identifier(:catalog_name || '.' || 'resource_bronze.tbl_antena')
SELECT
    msisdn,
    tx_uplink + cast(rand() * 10 - 5 as bigint) AS tx_uplink,
    tx_downlink + cast(rand() * 10 - 5 as bigint) AS tx_downlink,
    qt_volume + cast(rand() * 10 - 5 as bigint) AS qt_volume,
    qt_duration + cast(rand() * 10 - 5 as bigint) AS qt_duration,
    make_timestamp(
        year(to_date(:date_param)), 
        month(to_date(:date_param)), 
        day(to_date(:date_param)), 
        hour(cast(ds_start_time as timestamp)), 
        minute(cast(ds_start_time as timestamp)), 
        second(cast(ds_start_time as timestamp))
    ) AS dt_start_time,
    make_timestamp(
        year(to_date(:date_param)), 
        month(to_date(:date_param)), 
        day(to_date(:date_param)), 
        hour(cast(ds_end_time as timestamp)), 
        minute(cast(ds_end_time as timestamp)), 
        second(cast(ds_end_time as timestamp))
    ) AS dt_end_time,
    num_served_imei as nu_served_imei,
    cd_cgi,
    cd_pais,
    cd_area,
    to_date(:date_param) AS partition_date
FROM identifier(:catalog_name || '.' || 'misc.aux_tbl_antenna_pattern');