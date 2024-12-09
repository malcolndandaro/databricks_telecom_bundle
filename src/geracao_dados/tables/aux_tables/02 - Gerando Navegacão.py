# Databricks notebook source
# MAGIC %md 
# MAGIC # Gerando navegação
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

from pyspark.sql.functions import col, explode, arrays_zip

# COMMAND ----------

# Criando padrão de Navegação

padrao_navegacao = spark.sql(f"""
WITH count_cte AS (
    SELECT COUNT(*) AS cnt FROM {catalog_name}.misc.aux_tbl_clientes
),
numbers AS (
    SELECT explode(sequence(1, 32768)) AS n -- 2^15 = 32768, all possible 15-bit combinations
),
binary_rows AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY n) AS row_num,
        n
    FROM numbers
    WHERE n < POWER(2, 15) -- 2^15 = 32768, all possible 15-bit combinations
      AND BIT_COUNT(n) >=2 and BIT_COUNT(n) <= 8 
),
limited_rows AS (
    SELECT 
        row_num,
        n,
        (n & 16384) / 16384 AS col1,
        (n & 8192) / 8192 AS col2,
        (n & 4096) / 4096 AS col3,
        (n & 2048) / 2048 AS col4,
        (n & 1024) / 1024 AS col5,
        (n & 512) / 512 AS col6,
        (n & 256) / 256 AS col7,
        (n & 128) / 128 AS col8,
        (n & 64) / 64 AS col9,
        (n & 32) / 32 AS col10,
        (n & 16) / 16 AS col11,
        (n & 8) / 8 AS col12,
        (n & 4) / 4 AS col13,
        (n & 2) / 2 AS col14,
        (n & 1) AS col15
    FROM binary_rows
),
final_rows AS (
    SELECT 
        limited_rows.row_num,
        array(
            (n & 16384) / 16384,
            (n & 8192) / 8192,
            (n & 4096) / 4096,
            (n & 2048) / 2048,
            (n & 1024) / 1024,
            (n & 512) / 512,
            (n & 256) / 256,
            (n & 128) / 128,
            (n & 64) / 64,
            (n & 32) / 32,
            (n & 16) / 16,
            (n & 8) / 8,
            (n & 4) / 4,
            (n & 2) / 2,
            (n & 1)
        ) AS cols_array,
        count_cte.cnt,
        ROW_NUMBER() OVER (ORDER BY limited_rows.row_num) AS rn,
        array_join(
            filter(
                array(
                    CASE WHEN (n & 16384) / 16384 = 1 THEN 1 ELSE NULL END,
                    CASE WHEN (n & 8192) / 8192 = 1 THEN 2 ELSE NULL END,
                    CASE WHEN (n & 4096) / 4096 = 1 THEN 3 ELSE NULL END,
                    CASE WHEN (n & 2048) / 2048 = 1 THEN 4 ELSE NULL END,
                    CASE WHEN (n & 1024) / 1024 = 1 THEN 5 ELSE NULL END,
                    CASE WHEN (n & 512) / 512 = 1 THEN 6 ELSE NULL END,
                    CASE WHEN (n & 256) / 256 = 1 THEN 7 ELSE NULL END,
                    CASE WHEN (n & 128) / 128 = 1 THEN 8 ELSE NULL END,
                    CASE WHEN (n & 64) / 64 = 1 THEN 9 ELSE NULL END,
                    CASE WHEN (n & 32) / 32 = 1 THEN 10 ELSE NULL END,
                    CASE WHEN (n & 16) / 16 = 1 THEN 11 ELSE NULL END,
                    CASE WHEN (n & 8) / 8 = 1 THEN 12 ELSE NULL END,
                    CASE WHEN (n & 4) / 4 = 1 THEN 13 ELSE NULL END,
                    CASE WHEN (n & 2) / 2 = 1 THEN 14 ELSE NULL END,
                    CASE WHEN (n & 1) = 1 THEN 15 ELSE NULL END
                ),
                x -> x IS NOT NULL
            ),
            ','
        ) AS ones_positions
    FROM limited_rows
    CROSS JOIN count_cte
)
SELECT * 
FROM final_rows
WHERE rn <= (SELECT cnt FROM count_cte)""")

# COMMAND ----------

display(padrao_navegacao)

# COMMAND ----------

from pyspark.sql.functions import expr, rand, shuffle

padrao_navegacao = padrao_navegacao.withColumn(
    "categorias_id",
    expr("""
        transform(
            split(ones_positions, ','),
            x -> cast(x as int)
        )
    """)
).withColumn(
    "nav_categorias_id",
    expr("""
        flatten(
            transform(
                sequence(1, 34),
                x -> categorias_id
            )
        )
    """)
).withColumn(
    "nav_categorias_id",
    expr("slice(shuffle(nav_categorias_id), 1, 34)")
).withColumn(
    "site_id",
    expr("""
       transform(sequence(1, size(categorias_id)), x -> cast(rand() * 50 + 1 as int))
    """)
).withColumn(
    "site_id",
    shuffle(expr("site_id"))
).withColumn(
    "nav_site_id",
    expr("""
        transform(sequence(1, 34), x -> element_at(site_id, (x % size(site_id)) + 1))
    """)
)

display(padrao_navegacao)

# COMMAND ----------

padrao_navegacao = padrao_navegacao.select(
    col("row_num").alias("id"),
    "categorias_id",
    "nav_categorias_id",
    "nav_site_id"
)
num_comportamento = padrao_navegacao.count()
display(padrao_navegacao)

# COMMAND ----------

# df_msisdn = spark.read.table(f"{catalog_name}.misc.aux_tbl_clientes"
# ).select("nu_tlfn"
# ).withColumn("id_comportamento", (rand() * num_comportamento).cast("int")).limit(10000) # Somente para teste

df_msisdn = spark.read.table(f"{catalog_name}.misc.aux_tbl_clientes"
).select("nu_tlfn"
).withColumn("id_comportamento", (rand() * num_comportamento).cast("int"))

display(df_msisdn)

# COMMAND ----------

nav = df_msisdn.join(
    padrao_navegacao, 
    df_msisdn.id_comportamento == padrao_navegacao.id, 
    "inner"
).select(
    "nu_tlfn", 
    "nav_categorias_id",
    "nav_site_id"    
)
display(nav)

# COMMAND ----------

nav.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_tbl_padrao_de_navegacao")

# COMMAND ----------

dados_antenas = spark.read.table(f"{catalog_name}.misc.aux_tbl_antenna_pattern")
display(dados_antenas)

# COMMAND ----------

from pyspark.sql.functions import expr

nav = nav.withColumn("ds_ip", expr("concat(cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int))"))

# COMMAND ----------

display(nav)

# COMMAND ----------

aux_tbl_antenna_pattern = spark.table(f"{catalog_name}.misc.aux_tbl_antenna_pattern")

result = nav.join(aux_tbl_antenna_pattern, nav.nu_tlfn == aux_tbl_antenna_pattern.msisdn)

display(result)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "auto")

ptcl = [ "Youtube", "Instagram", "Whatsapp", "Likeding", "Facebook", "Twitter",  "Pinterest", "Reddit", "Tiktok", "Snapchat", "Tinder", "Viber", "Wechat", "Skype" , "Outros" 
]

sbpt = [ "Streaming", "Redes Sociais", "Mensagens", "Redes Sociais", "Redes Sociais", "Redes Sociais",  "Redes Sociais", "Redes Sociais", "Redes Sociais", "Mensagens", "Redes Sociais", "Mensagens", "Mensagens", "Mensagens", "Outros" ] 

ptcl_expr = f"""
CASE 
    WHEN nav_categorias_id = 15 THEN 
        CASE 
            {" ".join([f"WHEN nav_site_id % {len(ptcl)} = {i} THEN '{ptcl[i]}'" for i in range(len(ptcl))])}
            ELSE 'Outros'
        END
    ELSE CAST(nav_categorias_id AS STRING)
END
"""

sbpt_expr = f"""
CASE 
    WHEN nav_categorias_id = 15 THEN 
        CASE 
            {" ".join([f"WHEN nav_site_id % {len(sbpt)} = {i} THEN '{sbpt[i]}'" for i in range(len(sbpt))])}
            ELSE 'Outros'
        END
    ELSE CAST(nav_site_id AS STRING)
END
"""

result2 = result.select(
    col("nu_tlfn").alias("nr_tlfn"),
    "ds_ip",
    col("num_served_imei").alias("cd_imei"),
    "cd_cgi",
    col("ds_start_time").alias("dt_ini_nvgc"),
    col("ds_end_time").alias("dt_fim_nvgc"),
    (col("qt_volume") / 34).cast("integer").alias("qtdd_byte_tfgd"),
    explode( 
            arrays_zip("nav_categorias_id", "nav_site_id")
    ).alias("zipped")
).select(
    "nr_tlfn", "ds_ip", "cd_imei", "cd_cgi", "dt_ini_nvgc","dt_fim_nvgc", "qtdd_byte_tfgd", "zipped.*",
    expr("concat('https://www.site_', zipped.nav_categorias_id, '_', zipped.nav_site_id, '.com')").alias("ds_host"),
    expr(ptcl_expr).alias("ds_pctl"),
    expr(sbpt_expr).alias("ds_sbpt")
)

# COMMAND ----------

display(result2)

# COMMAND ----------

result2.write.mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_tbl_navegacao_pattern")
