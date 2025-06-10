# Databricks notebook source
p_catalog = spark.conf.get("confs.p_catalog")
p_schema = "misc"
p_table = "dim_date"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Premissas:
# MAGIC - Nome: dim_calendario
# MAGIC - Descrição da tabela: Tabela composta por variáveis qualitativas de tempo.
# MAGIC - Tipo: SCD-1

# COMMAND ----------

# Importa bibliotecas
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType, DataType
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# Importa bibliotecas
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType, DataType, DateType
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# Função para traduzir o nome do mês
def traduz_mes(mes):
    meses_pt_br = {
    'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
    'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
    'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
    'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
}
    return meses_pt_br.get(mes, mes)


# Função para traduzir o nome do mês abreviado
def traduz_mes_abrv(mes):
    meses_pt_br_abrv = {
    'Jan': 'Jan', 'Feb': 'Fev', 'Mar': 'Mar',
    'Apr': 'Abr', 'May': 'Mai', 'Jun': 'Jun',
    'Jul': 'Jul', 'Aug': 'Ago', 'Sep': 'Set',
    'Oct': 'Out', 'Nov': 'Nov', 'Dec': 'Dez'
}

    return meses_pt_br_abrv.get(mes, mes)

# ------------------------------------------------------------------------

# Função para traduzir o nome do mês em 'mes_ano'
def traduz_mes_ano(mes_ano):
    meses_pt_br_num = {
    '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março',
    '04': 'Abril', '05': 'Maio', '06': 'Junho',
    '07': 'Julho', '08': 'Agosto', '09': 'Setembro',
    '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
}
    mes, ano = mes_ano.split('-')
    mes_traduzido = meses_pt_br_num.get(mes)
    return f"{mes_traduzido}-{ano}"

# ------------------------------------------------------------------------

# Função para traduzir o nome do mês em 'mes_ano' (abreviado)
def traduz_mes_ano_abrv(mes_ano):
    meses_pt_br_num_abrv = {
    '01': 'Jan', '02': 'Fev', '03': 'Mar',
    '04': 'Abr', '05': 'Mai', '06': 'Jun',
    '07': 'Jul', '08': 'Ago', '09': 'Set',
    '10': 'Out', '11': 'Nov', '12': 'Dez'
}
    mes, ano = mes_ano.split('-')
    mes_traduzido = meses_pt_br_num_abrv.get(mes)
    return f"{mes_traduzido}-{ano}"

# ------------------------------------------------------------------------


def traduz_dia_semana(dia):

    # Mapeamento dos dias da semana para português
    dias_da_semana_pt_br = {
        'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    return dias_da_semana_pt_br.get(dia, dia)


# ------------------------------------------------------------------------
def traduz_dia_semana_abrv(dia):
    
    dias_da_semana_pt_br_abrv = {
    'Monday': 'Seg', 'Tuesday': 'Ter', 'Wednesday': 'Qua',
    'Thursday': 'Qui', 'Friday': 'Sex', 'Saturday': 'Sab', 'Sunday': 'Dom'
}
    return dias_da_semana_pt_br_abrv.get(dia, dia)


# ------------------------------------------------------------------------

# Formata trimestre
def formatar_trimestre(trimestre):
    return f"T{trimestre}"

# COMMAND ----------

# Define o intervalo de datas
data_inicio = "2000-01-01"
data_fim = "2099-01-01"

dataInicial = to_date(lit(data_inicio))            # Converte as strings de data para o tipo Date
dataFinal = to_date(lit(data_fim))              # Converte as strings de data para o tipo Date 

# Cria uma sequência de datas
df_datas = spark.range(1).select(explode(sequence(dataInicial, dataFinal)).alias('DATA'))

# Adiciona colunas necessárias
df_calendario = df_datas \
    .withColumn("SK_DIM_DATE", monotonically_increasing_id()) \
    .withColumn("ANO", year(col('DATA'))) \
    .withColumn("SEMANA_DO_ANO", dayofweek(col('DATA'))) \
    .withColumn("DIA_DO_ANO", dayofyear(col('DATA'))) \
    .withColumn("MES", month(col('DATA'))) \
    .withColumn("MES_ANO", date_format('DATA','MM-yyyy')) \
    .withColumn("NM_MES_ANO", date_format('DATA','MMM-yyyy')) \
    .withColumn("NM_MES", date_format('DATA','MMMM')) \
    .withColumn("NM_MES_ABRV", date_format('DATA','MMM')) \
    .withColumn("SEMESTRE", when(col("MES") <= 6, "S1").otherwise("S2")) \
    .withColumn("TRIMESTRE", quarter(col('DATA'))) \
    .withColumn("NM_DIA_SEMANA", date_format(col('DATA'), 'EEEE')) \
    .withColumn("NM_DIA_SEMANA_ABRV", date_format(col('DATA'), 'EEEE')) \
    .withColumn("DIA", dayofmonth(col('DATA'))) 

# Definição das UDFs para tradução dos nomes dos meses
traduz_mes_udf = udf(traduz_mes, StringType())
traduz_mes_abrv_udf = udf(traduz_mes_abrv, StringType())
traduz_mes_ano_udf = udf(traduz_mes_ano, StringType())
traduz_mes_ano_abrv_udf = udf(traduz_mes_ano_abrv, StringType())
traduz_dia_semana_udf = udf(traduz_dia_semana, StringType())
traduz_dia_semana_abrv_udf = udf(traduz_dia_semana_abrv, StringType())
formatar_trimestre_udf = udf(formatar_trimestre, StringType())
    
# Aplicação da UDF para tradução dos nomes dos meses
df_calendario = df_calendario \
    .withColumn("NM_MES", traduz_mes_udf(col("NM_MES"))) \
    .withColumn("NM_MES_ABRV", traduz_mes_abrv_udf(col("NM_MES_ABRV"))) \
    .withColumn("NM_MES_ANO", traduz_mes_ano_udf(col("MES_ANO"))) \
    .withColumn("NM_MES_ANO_ABRV", traduz_mes_ano_abrv_udf(col("MES_ANO"))) \
    .withColumn("NM_DIA_SEMANA", traduz_dia_semana_udf(col("NM_DIA_SEMANA"))) \
    .withColumn("NM_DIA_SEMANA_ABRV", traduz_dia_semana_abrv_udf(col("NM_DIA_SEMANA_ABRV"))) \
    .withColumn("TRIMESTRE", formatar_trimestre_udf(col('TRIMESTRE')))

# Insere as colunas default de dimensão
df_calendario = df_calendario.withColumn("ROW_INGESTION_TIMESTAMP", current_timestamp()) 
df_calendario = df_calendario.withColumn("SK_DIM_DATE", expr("CAST(DATE_FORMAT(CAST(DATA AS DATE), 'yyyyMMdd') AS INT)"))

# Ordena as colunas
df_calendario = df_calendario.select(
    "SK_DIM_DATE",
    "ROW_INGESTION_TIMESTAMP",
    "DATA",
    "ANO",
    "SEMESTRE",
    "TRIMESTRE",
    "MES",
    "SEMANA_DO_ANO",
    "NM_DIA_SEMANA",
    "NM_DIA_SEMANA_ABRV",
    "DIA_DO_ANO",
    "DIA",
    "MES_ANO",
    "NM_MES_ANO",
    "NM_MES_ANO_ABRV",
    "NM_MES",
    "NM_MES_ABRV"
)

# COMMAND ----------

import dlt

schema = """
    SK_DIM_DATE INT PRIMARY KEY COMMENT 'Chave Surrogada do Calendário',
    ROW_INGESTION_TIMESTAMP TIMESTAMP COMMENT 'Timestamp de Ingestao da Linha',
    DATA DATE COMMENT 'Data',
    ANO INT COMMENT 'Ano',
    SEMESTRE STRING COMMENT 'Semestre',
    TRIMESTRE STRING COMMENT 'Trimestre',
    MES INT COMMENT 'Mês',
    SEMANA_DO_ANO INT COMMENT 'Semana do Ano',
    NM_DIA_SEMANA STRING COMMENT 'Nome do Dia da Semana',
    NM_DIA_SEMANA_ABRV STRING COMMENT 'Nome Abreviado do Dia da Semana',
    DIA_DO_ANO INT COMMENT 'Dia do Ano',
    DIA INT COMMENT 'Dia',
    MES_ANO STRING COMMENT 'Mês e Ano',
    NM_MES_ANO STRING COMMENT 'Nome do Mês e Ano',
    NM_MES_ANO_ABRV STRING COMMENT 'Nome Abreviado do Mês e Ano',
    NM_MES STRING COMMENT 'Nome do Mês',
    NM_MES_ABRV STRING COMMENT 'Nome Abreviado do Mês'
"""



# Parâmetros
mode_write = 'overwrite'
catalog_name = p_catalog
schema_name = p_schema
table_name = p_table

@dlt.table(
    name=f'{table_name}',
    comment="Tabela de calendário com informações detalhadas sobre datas",
    schema=schema
    
)
def calendario():
        return df_calendario

