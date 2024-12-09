# Databricks notebook source
dbutils.widgets.text("p_catalog", "prod")
dbutils.widgets.text("p_eventhub", "vivo-prod-navegacao")
dbutils.widgets.text("p_executar_segundos", "120")
dbutils.widgets.text("p_schema_table", "resource_bronze.tbl_antena")


# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")
p_eventhub = dbutils.widgets.get("p_eventhub")
p_executar_segundos = int(dbutils.widgets.get("p_executar_segundos"))
p_schema_table = dbutils.widgets.get("p_schema_table")

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    collect_list,
    struct,
    expr,
    ceil,
    from_json,
    explode,
    current_timestamp
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType
import json, time

# COMMAND ----------


connectionString = f"Endpoint=sb://vivo-event-hubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=OcbvvJySkKzKbkjJX70YzZQQvxejLvL1C+AEhDyfzAc=;EntityPath={p_eventhub}"
ehconf = {}
ehconf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Start from beginning of stream
startOffset = "-1"# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
ehconf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
if p_catalog in ["dev", "qa", "sandbox"]:
  ehconf["maxEventsPerTrigger"] = 1
else:
  ehconf["maxEventsPerTrigger"] = 40000



# COMMAND ----------

avro_schema = """
{
  "type": "array",
  "items": {
    "type": "record",
    "name": "DataRecord",
    "fields": [
      {"name": "msisdn", "type": "string"},
      {"name": "tx_uplink", "type": ["null", "long"]},
      {"name": "tx_downlink", "type": ["null", "long"]},
      {"name": "qt_volume", "type": ["null", "long"]},
      {"name": "qt_duration", "type": ["null", "long"]},
      {"name": "dt_start_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      {"name": "dt_end_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      {"name": "nu_served_imei", "type": ["null", "string"]},
      {"name": "cd_cgi", "type": ["null", "string"]},
      {"name": "cd_pais", "type": "string"},
      {"name": "cd_area", "type": "string"}
    ]
  }
}
"""

# COMMAND ----------

checkpoint_path = f"/tmp/malcoln/checkdelta_antena_{p_catalog}/{p_eventhub}"
table_name = f"{p_catalog}.{p_schema_table}"
dbutils.fs.rm(checkpoint_path, recurse=True)
if table_name.split(".")[2] == 'antena_test_ehub':
    spark.sql(f"drop table if exists {table_name}")

# COMMAND ----------

def parse_eventhub(df) -> DataFrame:
    df = (
        df
        .select(    
            from_avro("body", str(avro_schema)).alias("rows")
            )
        .select(explode("rows").alias("col"))
        .select("col.*")
    )
    return df

# COMMAND ----------

display(
    spark.sql(
        f"""
        select count(*) from {table_name}
        where partition_date in ('2024-10-01', '2024-30-09', '2024-10-02')
"""
    )
)

# COMMAND ----------

df_eventhubs = (
    spark.readStream
    .format("eventhubs")
    .options(**ehconf)
    .load()
)

# COMMAND ----------


start_time = time.time()


load_from_eventhubs = df_eventhubs \
.transform(parse_eventhub) \
.withColumn("partition_date", expr("to_date(dt_start_time, 'yyy-MM-dd')")) \
.writeStream \
.queryName("load_from_eventhubs") \
.format("delta") \
.option("checkpointLocation", checkpoint_path) \
.toTable(table_name)



# COMMAND ----------

execution_time = 0
execution_time_target = round(p_executar_segundos)
while execution_time < execution_time_target:
    time.sleep(10)
    end_time = time.time()
    execution_time = round(end_time - start_time)
    print(f"Stream rodando por {execution_time} segundos, aguardando até {execution_time_target} segundos, faltam {execution_time_target - execution_time} segundos")
load_from_eventhubs.stop()
end_time = time.time()
execution_time = end_time - start_time
print(f"Stream encerrada após {execution_time} segundos")

# COMMAND ----------

display(
    spark.sql(f"""
              select count(*) as total_rows, 
              sum(octet_length(concat(*)))/(1024*1024) as total_mb,
              {execution_time} as execution_time_seconds,
              total_mb/execution_time_seconds as mb_per_second,
              (mb_per_second*60)/1024 as gb_per_minute
              from {table_name}
              where partition_date in ('2024-10-01', '2024-30-09', '2024-10-02')
              """)
)
