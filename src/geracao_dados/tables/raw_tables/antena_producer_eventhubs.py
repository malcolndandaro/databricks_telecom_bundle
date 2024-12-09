# Databricks notebook source
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    collect_list,
    struct,
    expr,
    ceil
)
from pyspark.sql.avro.functions import to_avro
from pyspark.sql import DataFrame

# COMMAND ----------

dbutils.widgets.text("p_eventhub", "vivo-dev-navegacao")
dbutils.widgets.text("p_catalog", "dev")
dbutils.widgets.text("p_partition_date", "2024-10-01")

# COMMAND ----------

p_eventhub = dbutils.widgets.get("p_eventhub")
p_catalog = dbutils.widgets.get("p_catalog")
p_partition_date = dbutils.widgets.get("p_partition_date")

# COMMAND ----------

import json
connectionString = f"Endpoint=sb://vivo-event-hubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=OcbvvJySkKzKbkjJX70YzZQQvxejLvL1C+AEhDyfzAc=;EntityPath={p_eventhub}"
ehconf = {}
ehconf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

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

group_size = 10500


# COMMAND ----------

def process_antena(df: DataFrame, batch_id: int) -> DataFrame:
    (
        df
        .withColumn("id", monotonically_increasing_id())
        .withColumn("row_group", ceil(col("id") / group_size))
        .groupBy("row_group")
        .agg(
            (
                collect_list(
                    struct(
                        [
                            "msisdn",
                            "tx_uplink",
                            "tx_downlink",
                            "qt_volume",
                            "qt_duration",
                            "dt_start_time",
                            "dt_end_time",
                            "nu_served_imei",
                            "cd_cgi",
                            "cd_pais",
                            "cd_area"
                        ]
                    )
                )
            ).alias("rows")
        )
        .select(to_avro("rows", avro_schema).alias("body"))
        .write.format("eventhubs")
        .options(**ehconf)
        .save()
    )

# COMMAND ----------

df_delta = (
    spark
    .readStream.
    option("maxBytesPerTrigger", "1G")
    .table(f"{p_catalog}.misc.tbl_antena")
)

partition_date = p_partition_date

df_delta = df_delta.filter(f"partition_date = '{partition_date}' ")

# COMMAND ----------

# DBTITLE 1,truncate
(   df_delta
    .writeStream
    .foreachBatch(process_antena)
    .trigger(availableNow=True)
    .start()
 )
