# Databricks notebook source
# MAGIC %md
# MAGIC ### DEFINE CONNECTION DETAILS TO SQL SERVER

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
# MAGIC import java.sql.{Connection, DriverManager}
# MAGIC
# MAGIC dbutils.widgets.text("server", "", "SQL Server Hostname")
# MAGIC val server = dbutils.widgets.get("server")
# MAGIC dbutils.widgets.text("database", "", "SQL Server database")
# MAGIC val database = dbutils.widgets.get("database")
# MAGIC dbutils.widgets.text("data_size", "small", "Data size (small=5%, medium=25%, large=100%)")
# MAGIC val data_size = dbutils.widgets.get("data_size")
# MAGIC
# MAGIC val user = "vivoadm"
# MAGIC val password = "S-29ms*YD%+;$uyxwzhrNp"
# MAGIC val jdbcUrl = s"jdbc:sqlserver://$server.database.windows.net:1433;database=$database;user=$user;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLEAN TABLE IN SOURCE SYSTEM (DROP)

# COMMAND ----------

# MAGIC %scala  
# MAGIC //var connection: Connection = null
# MAGIC //try {
# MAGIC //  Class.forName(driver)
# MAGIC //  connection = DriverManager.getConnection(jdbcUrl, user, password)
# MAGIC //  val statement = connection.createStatement()
# MAGIC //  val sql = "DROP TABLE IF EXISTS dbo.atendimento"
# MAGIC //  val res = statement.execute(sql)
# MAGIC //  println(s"Result = $res")
# MAGIC //} catch {
# MAGIC //  case e: Exception => e.printStackTrace()
# MAGIC //} finally {
# MAGIC //  connection.close()
# MAGIC //}

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE ATENDIMENTO TABLE IN SOURCE SQL SERVER

# COMMAND ----------

# MAGIC %scala
# MAGIC val create_table = """
# MAGIC CREATE TABLE dbo.src_atendimento (
# MAGIC   msisdn VARCHAR(13),
# MAGIC   cpf CHAR(14),
# MAGIC   idatendimento BIGINT NOT NULL IDENTITY (1,1) PRIMARY KEY,
# MAGIC   dtabertura DATETIME,
# MAGIC   dtprazofinalinterno DATETIME,
# MAGIC   qtinsistencia BIGINT,
# MAGIC   inalarme BIGINT,
# MAGIC   icanal VARCHAR(4),
# MAGIC   dtultimaalteracao DATETIME,
# MAGIC   nivel VARCHAR(5),
# MAGIC   dtprazofinalanatel DATETIME,
# MAGIC   qthorasprazoatendimento BIGINT,
# MAGIC   dsobservacao VARCHAR(1000),
# MAGIC   nrprotocolo BIGINT)
# MAGIC """
# MAGIC
# MAGIC var connection: Connection = null
# MAGIC try {
# MAGIC   Class.forName(driver)
# MAGIC   connection = DriverManager.getConnection(jdbcUrl, user, password)
# MAGIC   val statement = connection.createStatement()
# MAGIC   val res = statement.execute(create_table)
# MAGIC   println(s"Result = $res")
# MAGIC } catch {
# MAGIC   case e: Exception => e.printStackTrace()
# MAGIC } finally {
# MAGIC   connection.close()
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable CDC in Database

# COMMAND ----------

# MAGIC %scala  
# MAGIC var connection: Connection = null
# MAGIC try {
# MAGIC   Class.forName(driver)
# MAGIC   connection = DriverManager.getConnection(jdbcUrl, user, password)
# MAGIC   val statement = connection.createStatement()
# MAGIC   val sql = "EXEC sys.sp_cdc_enable_db"
# MAGIC   val res = statement.execute(sql)
# MAGIC   println(s"Result = $res")
# MAGIC } catch {
# MAGIC   case e: Exception => e.printStackTrace()
# MAGIC } finally {
# MAGIC   connection.close()
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable cdc in table 

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.{Connection, DriverManager}
# MAGIC
# MAGIC var connection: Connection = null
# MAGIC try {
# MAGIC   Class.forName(driver)
# MAGIC   connection = DriverManager.getConnection(jdbcUrl, user, password)
# MAGIC   val statement = connection.createStatement()
# MAGIC   val sql = """ EXEC sys.sp_cdc_enable_table  
# MAGIC     @source_schema = N'dbo',  
# MAGIC     @source_name   = N'src_atendimento',  
# MAGIC     @role_name     = NULL, 
# MAGIC     @supports_net_changes = 1
# MAGIC   """
# MAGIC   val res = statement.execute(sql)
# MAGIC   println(s"Result = $res")
# MAGIC } catch {
# MAGIC   case e: Exception => e.printStackTrace()
# MAGIC } finally {
# MAGIC   connection.close()
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Random data

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.functions import row_number, expr, col, when, lit, rand, current_timestamp, unix_timestamp, ceil
#from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("p_catalog", "", "Catalog Name")
p_catalog = dbutils.widgets.get("p_catalog")
database = dbutils.widgets.get("database")
table_aux_tbl_clientes = f"{p_catalog}.misc.aux_tbl_clientes"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - dtprazofinalinterno should be 24 or 48
# MAGIC - qthorasprazoatendimento should not be consistent with dtprazofinalinterno (should have some nulls in there) 
# MAGIC - ATENDIMENTO_TIME_IN_HOURS ranging from 1 to 72 should be fine
# MAGIC - IS_CLOSED = random boolean
# MAGIC - inalarme = (dtabertura + ATENDIMENTO_TIME_IN_HOURS > dtprazofinalinterno) ? 1 : 0
# MAGIC - qthorasprazoatendimento = IS_CLOSED ? ATENDIMENTO_TIME : NULL
# MAGIC
# MAGIC - qthorasprazoatendimento NULL means it still open
# MAGIC - inalarme is assumed to be updated by the source and it tells if the atendimento is delayed (calculate for data generation as current_timestamp - dtabertura > dtprazofinalinterno - dtabertura)

# COMMAND ----------

# Import the data size utility functions
%run ../aux_functions/data_size_utils

original_rows = 1500000
data_rows = calculate_data_rows(original_rows, data_size)

## msisdn = nu_tlfn (aux_tbl_clientes)
## cpf = nu_doct (aux_tbl_clientes)
## idatendimento PRIMARY KEY IDENTITY (1,1) ---> In Source Database
## September 1 - 30


generation_spec = (
    dg.DataGenerator(spark, rows=data_rows)
    .withColumn('qtinsistencia', 'bigint', minValue=0, maxValue=10, random=True)
    .withColumn('icanal', 'string', values=["AURA", "URA", "APP", "LOJA"], random=True)
    .withColumn('nivel', 'string', values=["BAIXO", "MÉDIO", "ALTO"], random=True)
    .withColumn('dsobservacao', 'string', template=r'\\w')
)

df_atendimento = (generation_spec.build()
                    .withColumn('dtabertura',
                                expr("""
                                    from_unixtime(
                                        unix_timestamp('2024-09-01 01:00:00') + 
                                        cast(rand() * (unix_timestamp('2024-09-30 23:59:00') - unix_timestamp('2024-09-01 01:00:00')) as bigint)
                                    )
                                """))
                    .withColumn('dtprazofinalinterno',  expr("dateadd(hour, cast(rand() * (72 - 2) + 2 as int), dtabertura)"))
                    .withColumn('expected_time', expr("ceil((unix_timestamp(dtprazofinalinterno) - unix_timestamp(dtabertura)) / 3600.0)"))
                    .withColumn('qthorasprazoatendimento', expr("CASE WHEN rand() < 0.2 THEN NULL ELSE cast(rand() * (80 - 1) + 1 as int) END"))
                    .withColumn("random_date",
                                expr("""
                                    from_unixtime(
                                        unix_timestamp('2024-10-02 00:00:01') + 
                                        cast(rand() * (unix_timestamp(dtabertura) - unix_timestamp('2024-10-02 00:00:01')) as bigint)
                                    )
                                """))
                    .withColumn('dtultimaalteracao', expr("dateadd(hour, 2, dtabertura)"))
                    .withColumn('dtprazofinalanatel', expr("dateadd(day, 5, dtprazofinalinterno)"))
                    .withColumn("inalarme",
                                expr("""CASE 
                                    WHEN qthorasprazoatendimento IS NULL THEN 
                                        CASE 
                                            WHEN ceil((unix_timestamp(random_date) - unix_timestamp(dtabertura)) / 3600.0) > 
                                                ceil((unix_timestamp(dtprazofinalinterno) - unix_timestamp(dtabertura)) / 3600.0) 
                                            THEN 1 ELSE 0 END
                                    ELSE 
                                        CASE WHEN qthorasprazoatendimento > ceil((unix_timestamp(dtprazofinalinterno) - unix_timestamp(dtabertura)) / 3600.0) 
                                        THEN 1 ELSE 0 END
                                END"""))
                    .withColumn('nrprotocolo', expr("cast(cast(unix_timestamp(dtabertura) as string) as bigint)"))
)



# COMMAND ----------

df_aux_tbl_clientes = spark.read.table(table_aux_tbl_clientes).select(col("nu_tlfn").alias("msisdn"), col("nu_doct").alias("cpf"))

df_atendimento = df_atendimento.withColumn("row_number", expr("row_number() over (order by rand())"))
df_aux_tbl_clientes = df_aux_tbl_clientes.withColumn("row_number", expr("row_number() over (order by rand())"))

# COMMAND ----------

df = df_atendimento.join(df_aux_tbl_clientes, on="row_number", how="inner")

# COMMAND ----------

#columns = spark.read.table(f"dev_atendimento.dbo.atendimento").columns
columns = ['msisdn',
 'cpf',
 'dtabertura',
 'dtprazofinalinterno',
 'qtinsistencia',
 'inalarme',
 'icanal',
 'dtultimaalteracao',
 'nivel',
 'dtprazofinalanatel',
 'qthorasprazoatendimento',
 'dsobservacao',
 'nrprotocolo']
 
df = df.select(columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mutate random records to introduce data quality issues

# COMMAND ----------

# Parameters for mutation rates
mutation_rate_dtabertura = 0.05  # 5% of records will have dtabertura issues
mutation_rate_dtprazofinalinterno = 0.05  # 5% of records will have dtprazofinalinterno issues
mutation_rate_icanal = 0.1  # 10% of records will have icanal issues
mutation_rate_nivel = 0.05  # 5% of records will have nivel issues
mutation_rate_nrprotocolo = 0.05  # 5% of records will have nrprotocolo issues

# COMMAND ----------

# Introduce data quality issues
df_mutated = df.withColumn(
    'dtabertura',
    when(rand() < mutation_rate_dtabertura, expr("date_add(current_timestamp(), 30)")).otherwise(col('dtabertura'))
).withColumn(
    'dtprazofinalinterno',
    when(rand() < mutation_rate_dtprazofinalinterno, expr("date_sub(dtabertura, 1)")).otherwise(col('dtprazofinalinterno'))
).withColumn(
    'icanal',
    when(rand() < mutation_rate_icanal, lit("BAD")).otherwise(col('icanal'))
).withColumn(
    'nivel',
    when(rand() < mutation_rate_nivel, lit("BAD")).otherwise(col('nivel'))
).withColumn(
    'nrprotocolo',
    when(rand() < mutation_rate_nrprotocolo, lit(None)).otherwise(col('nrprotocolo'))
)

#display(df_mutated)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write dataframe to source sql server table

# COMMAND ----------

# Set up connection properties for Azure SQL Server
jdbcHostname = "vivo-sql-server.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = f"{database}"
jdbcUsername = "vivoadm"
jdbcPassword = "S-29ms*YD%+;$uyxwzhrNp"

connection_properties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "false",
    "hostNameInCertificate": "*.database.windows.net",
    "loginTimeout": "30"
}

# Define JDBC URL
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

# Write DataFrame to Azure SQL Database
df_mutated.write.option("batchsize", 10000).jdbc(url=jdbcUrl, table="src_atendimento", mode="append", properties=connection_properties)
