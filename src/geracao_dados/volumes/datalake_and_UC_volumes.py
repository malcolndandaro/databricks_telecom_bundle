# Databricks notebook source
# DBTITLE 1,Parameters
# Copy from Unity external location
dbutils.widgets.text("p_catalog", "databricks_telecom_bundle")
dbutils.widgets.text("p_schema", "ingestion")
dbutils.widgets.text("p_volume", "raw_data")

# COMMAND ----------

p_catalog = dbutils.widgets.get("p_catalog")
p_schema = dbutils.widgets.get("p_schema")
p_volume = dbutils.widgets.get("p_volume")

# COMMAND ----------

volume_name = f"{p_catalog}.{p_schema}.{p_volume}"
volume_path = f"/Volumes/{p_catalog}/{p_schema}/{p_volume}"
volume_uri = f"abfss://catalog-{p_catalog}@vivodatastorage.dfs.core.windows.net/{p_schema}/{p_volume}"



# COMMAND ----------

# DBTITLE 1,Grant permission
spark.sql(f""" CREATE CATALOG IF NOT EXISTS {p_catalog}""")
# Schema creation moved to bundle definition
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {volume_name} """)

# COMMAND ----------

# DBTITLE 1,List of domains
domains = [
    "billing", "customer", "marketing", "resource", "misc"
]

# COMMAND ----------

dirs = dbutils.fs.ls(volume_path)

# COMMAND ----------

# DBTITLE 1,Create dirs and volumes
dirs = [x.name for x in dirs]
if "billing/" not in dirs:
    print("Structure does not exists. Creating structure")
    for domain in domains:
        print("Creating domain: " + domain)
        dbutils.fs.mkdirs(f"{volume_path}/{domain}")
        print("Created domain:" f"{volume_path}/{domain}")
else:
    print("Datalake structure already exists. Skipping")
