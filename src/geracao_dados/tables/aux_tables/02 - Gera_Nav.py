# Databricks notebook source
# MAGIC %md 
# MAGIC # Gerando navegação
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog_name","ctakamiya_bundle", "Nome do catálogo")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

from pyspark.sql.functions import col, explode, arrays_zip, expr

# COMMAND ----------

# Creating centroids (100 centroids)

centroids = spark.sql(f"""
WITH numbers AS (
    SELECT explode(sequence(1, 32768)) AS n
),
binary_rows AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY n) AS row_num,
        n
    FROM numbers
    WHERE n < POWER(2, 15)
      AND BIT_COUNT(n) >= 3 AND BIT_COUNT(n) <= 10
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
        ARRAY(
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT),
            CAST(RAND() * 5 + 1 AS INT)
        ) AS other_categories_weights,
        ARRAY(
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT),
            CAST(RAND() * 50 + 50 AS INT)
        ) AS main_categories_weights
    FROM limited_rows
),
categories_calculation AS (
    SELECT 
        zip_with(
            transform(main_categories_weights, x -> CAST(x AS DOUBLE)),
            transform(cols_array, x -> CAST(x AS DOUBLE)),
            (x, y) -> x * y
        ) as main_categories,
        zip_with(
            transform(other_categories_weights, x -> CAST(x AS DOUBLE)),
            cols_array,
            (x, y) -> x * (CAST(y AS INT) ^ 1)
        ) as other_categories
    FROM final_rows
),
categories_result AS (
    SELECT 
        zip_with(
            main_categories,
            other_categories,
            (x, y) -> x + y
        ) as categories
    FROM categories_calculation
),
final_result AS (
    SELECT 
        categories,
        TRANSFORM(categories, x -> x / (AGGREGATE(categories, 0, (acc, y) -> acc + CAST(y AS INT)))) AS categories_percentage,
        TRANSFORM(categories_percentage, x -> CEIL(x * 15)) AS centroids_coord,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM categories_result
)
SELECT id, categories, categories_percentage, centroids_coord
FROM final_result
TABLESAMPLE (100 ROWS)
""")

# COMMAND ----------

display(centroids)

# COMMAND ----------

padrao_navegacao = spark.table(f"{catalog_name}.misc.aux_tbl_clientes") \
    .withColumn("centroid_id", expr(f"floor(rand() * {centroids.count()}) + 1")) \
    .join(
        centroids.withColumnRenamed("id", "centroid_id"),
        on="centroid_id",
        how="left"
    ).withColumn("new_coords", 
                 expr("""
                      transform(centroids_coord, x -> ABS(x + CAST(RAND() * 4 - 2 AS INT)))
                      """)
    ).withColumn("site_id", 
                 expr("""
                      transform(centroids_coord, x -> CAST(RAND() * 50 + 1 AS INT))
                      """)                      
    ).withColumn("nav_categorias_id",
                 expr("""
                      flatten(
                      transform(sequence(1, 15), 
                           x -> array_repeat(x, CAST(element_at(new_coords, x) AS INT)))
                      )
                      """)
    ).withColumn("nav_site_id",
                 expr("""
                      flatten(
                      transform(sequence(1, 15), 
                           x -> array_repeat(
                                CAST(element_at(site_id, x) AS INT), 
                                CAST(element_at(new_coords, x) AS INT)))
                      )
                      """)
    )
display(padrao_navegacao)

# COMMAND ----------

display(padrao_navegacao.filter("centroid_id = 7"))

# COMMAND ----------

dados_antenas = spark.read.table(f"{catalog_name}.misc.aux_tbl_antenna_pattern")
display(dados_antenas)

# COMMAND ----------

from pyspark.sql.functions import expr

nav = padrao_navegacao.withColumn("ds_ip", expr("concat(cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int), '.', cast(floor(rand() * 256) as int))"))

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
