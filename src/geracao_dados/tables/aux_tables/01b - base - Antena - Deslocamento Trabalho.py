# Databricks notebook source
# MAGIC %pip install faiss-cpu==1.8.0.post1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col, lit, unix_timestamp, expr, to_timestamp, array, explode, rand, from_unixtime
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd
import numpy as np
from datetime import datetime
import torch
import faiss


# COMMAND ----------

dbutils.widgets.text("departure_time", "6","Hora de saída")
dbutils.widgets.text("arrival_time", "9","Hora de chegada")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data de Referência")
dbutils.widgets.text("qt_GB_month", "25","GB p/ Mês")
dbutils.widgets.text("displcment_number", "5","Número de deslocamentos")
dbutils.widgets.text("data_ref", "2024-10-01 00:00:00","Data Referência")
dbutils.widgets.text("catalog_name","dev", "Nome do catálogo")

data_ref            = dbutils.widgets.get("data_ref")
departure_time      = int(dbutils.widgets.get("departure_time"))
arrival_time        = int(dbutils.widgets.get("arrival_time"))
qt_GB_month         = int(dbutils.widgets.get("qt_GB_month"))
num_displ           = int(dbutils.widgets.get("displcment_number"))
data_ref            = dbutils.widgets.get("data_ref")
catalog_name        = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga de cliente e endereços
# MAGIC
# MAGIC Carregando cliente e endereços gerado no notebook anteriro:
# MAGIC https://adb-6120542968195864.4.azuredatabricks.net/?o=6120542968195864#notebook/316131127233056/command/316131127233244
# MAGIC
# MAGIC

# COMMAND ----------

client_location = spark.read.table(f'{catalog_name}.misc.aux_tbl_cliente_localizacao')

# COMMAND ----------

client_location1 = client_location.select("nu_tlfn",
                                         col("cd_cgi"), 
                                         "nu_imei_aprl", 
                                         col("cd_area"),                                         
                                         col("lon_res"), 
                                         col("lat_res"))
display(client_location1)

                                

# COMMAND ----------

num_clients = client_location.count()
num_rows_antenna = 13_000_000_000
num_rows_antenna_per_client = num_rows_antenna / num_clients

# Calculate the volume of traffic for the morning period, assuming 10% of the total monthly traffic is used per day.
volume_traffic_commute = (qt_GB_month / 30) * 0.15

# Assuming the number of rows is uniformly distributed throughout the day, with variations only in tx_uplink and tx_down.
num_sess = (arrival_time - departure_time) * 100
commute_tx_down = (volume_traffic_commute * 1024 **3) / (num_sess * 50)
commute_tx_up = 0.15 * commute_tx_down


# COMMAND ----------

import faiss
import pickle
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Broadcast the index and ID map to all workers
index_file_path = f"/Volumes/{catalog_name}/misc/erbs/index_erb.bin"
id_coord_file_path = f"/Volumes/{catalog_name}/misc/erbs/id_coord.pkl"
ids_file_path = f"/Volumes/{catalog_name}/misc/erbs/ids.pkl"

faiss_index = faiss.read_index(index_file_path)
faiss_coord = None
with open(id_coord_file_path, 'rb') as f:
    faiss_coord = pickle.load(f)
faiss_ids = None
with open(ids_file_path, 'rb') as f:
    faiss_ids = pickle.load(f)

faiss_index.reset()
id_map = faiss.IndexIDMap(faiss_index)
id_map.add_with_ids(faiss_coord, faiss_ids)

index_broadcast = spark.sparkContext.broadcast(id_map)

# Function to retrieve a batch of queries
@pandas_udf("struct<query: array<double>, retrieved_id: int, distance: double>")
def retrieve_batch_queries(queries: pd.Series) -> pd.DataFrame:
    # Load the FAISS index and ID mapping from broadcast variables
    index = index_broadcast.value
    
    # Convert queries to numpy array and cut the last coordinate
    query_vectors = np.stack(queries.to_numpy())[:, :2]
    
    # Perform the search
    distances, indices = index.search(query_vectors, k=1)
    
    # Map indices to IDs, ensuring indices are within bounds
    retrieved_ids = []
    for idx in indices.flatten():
        if (len(indices.flatten()) > 0):            
            retrieved_ids.append(idx)
        
    # Create a DataFrame with the results
    results_df = pd.DataFrame({
        'query': queries,
        'retrieved_id': retrieved_ids,
        'distance': distances.flatten()
    })
    
    return results_df


#@pandas_udf("array<double>")
@pandas_udf("struct<query: array<array<double>>, retrieved_id: array<int>, distance: array<double>>")
# @pandas_udf("array<struct <query: array<array<double>>, retrieved_id: array<int>, distance: array<double>>>")
def retrieve_erb_nearst(pos: pd.Series) -> pd.DataFrame:
    def retrieve_batch_queries_int(queries: pd.Series) -> pd.DataFrame:
        # Load the FAISS index and ID mapping from broadcast variables
        index = index_broadcast.value
        
        # Convert queries to numpy array and cut the last coordinate
        query_vectors = np.stack(queries.to_numpy())[:, :2]
        
        # Perform the search
        distances, indices = index.search(query_vectors, k=1)
        
        # Map indices to IDs, ensuring indices are within bounds
        retrieved_ids = []
        for idx in indices.flatten():
            if (len(indices.flatten()) > 0):
                retrieved_ids.append(idx)
            
        # Create a DataFrame with the results
        results_df = pd.DataFrame({
            'query': queries,
            'retrieved_id': retrieved_ids,
            'distance': distances.flatten()
        })
        
        return results_df

    queries = [v for v in pos]
    retr_id = np.zeros((len(pos), num_disp))
    dist_id = np.zeros((len(pos), num_disp))
    ret = None
    for desloc in range(num_disp):
        vpos = [v[desloc][:2] for v in pos]
        ret = retrieve_batch_queries_int(pd.Series(vpos))
        retr_id[:, desloc] = ret['retrieved_id']
        dist_id[:, desloc] = ret['distance']
    results_df = pd.DataFrame({
            'query': queries,
            'retrieved_id': retr_id,
            'distance': dist_id.flatten()
        })
    return results_df


# COMMAND ----------

@pandas_udf(ArrayType(DoubleType()))
def simu_displacement(lat: pd.Series, lon: pd.Series) -> pd.Series:    
    nrow = lat.shape[0]
    num_angs_total = nrow * num_displ
    ang = np.radians([0, 30, 60, 90, 120, 150, 180])
    angs = ang[np.random.randint(0, len(ang), num_angs_total)].reshape(nrow, num_displ)
    disp = np.random.normal(20, 2, num_angs_total).reshape(nrow, num_displ)
    cos = np.cos(angs)
    sin = np.sin(angs)

    lat_displ = sin * (disp / 111.11)
    lon_displ = cos * (disp / 111.11)
    
    # Creating an array of [ [lon, lat, 1] ] for each row
    coords = np.array([[lon[i], lat[i], 1] for i in range(nrow)])

    R = np.zeros ( (nrow, num_displ, 3, 3))

    # Creating translation matrix for each displacement
    # The creation is vectorized
    R[:,:,0,0] = 1
    R[:,:,1,1] = 1
    R[:,:,0,2] = lat_displ
    R[:,:,1,2] = lon_displ
    R[:,:,2,2] = 1

    R = R.reshape(nrow, num_displ * 3 * 3)
    
    ret = pd.Series([R[i] for i in range(R.shape[0])])
    return ret

@pandas_udf("array<double>")
def cal_pos_final_batched(matrix: pd.Series, lat: pd.Series, lon: pd.Series) -> pd.Series:

    def block_diag_matmul_batched(A, B):
        n_blocks = A.shape[0]
        block_size = A.shape[1]
        
        # Reshape A and B into batches of block matrices
        A_batched = A.view((n_blocks, block_size, block_size))
        B_batched = B.view((n_blocks, 3, -1))
        
        # Perform batched matrix multiplication
        result_batched = torch.bmm(A_batched, B_batched)
        
        # Reshape the result back to original dimensions
        return result_batched.view((A.shape[0], -1))

    nrow = len(lat)
    coords = torch.tensor([[lon[i], lat[i], 1] for i in range(nrow)])
    coords.reshape(nrow, 1, 3, 1)
    mat = torch.tensor(np.array([matrix[i] for i in range(matrix.shape[0])]))
    
    mat = mat.reshape(nrow, num_displ, 3, 3)

    for i in range(num_displ):
        A_batched = mat[:,i,:,:]
        ret = block_diag_matmul_batched(A_batched, coords)
        coords = ret
    return pd.Series(ret.numpy().tolist())   

@pandas_udf("array<array<double>>")
def cal_pos_inter_batched(matrix: pd.Series, lat: pd.Series, lon: pd.Series) -> pd.Series:
    def block_diag_matmul_batched(A, B):
        n_blocks = A.shape[0]
        block_size = A.shape[1]
        
        # Reshape A and B into batches of block matrices
        A_batched = A.view((n_blocks, block_size, block_size))
        B_batched = B.view((n_blocks, 3, -1))
        
        # Perform batched matrix multiplication
        result_batched = torch.bmm(A_batched, B_batched)
        
        # Reshape the result back to original dimensions
        return result_batched.view((A.shape[0], -1))

    nrow = len(lat)
    coords = torch.tensor([[lon[i], lat[i], 1] for i in range(nrow)])
    coords.reshape(nrow, 1, 3, 1)
    mat = torch.tensor(np.array([matrix[i] for i in range(matrix.shape[0])]))
    
    mat = mat.reshape(nrow, num_displ, 3, 3)

    ret_inter = torch.zeros(nrow, num_displ, 3)

    for i in range(num_displ):
        A_batched = mat[:,i,:,:]
        ret = block_diag_matmul_batched(A_batched, coords)
        coords = ret
        ret_inter[:,i,:] = ret 
    return pd.Series(ret_inter.numpy().tolist()) 


    

# COMMAND ----------

deslocamento = client_location.withColumn(
     'deslocamentos', simu_displacement(col('lat_res'), col('lon_res'))
     ).withColumn('pos_final', cal_pos_final_batched(col('deslocamentos'), col('lat_res'), col('lon_res'))
     ).withColumn('pos_inter', cal_pos_inter_batched(col('deslocamentos'), col('lat_res'), col('lon_res')))


# COMMAND ----------

display(deslocamento)

# COMMAND ----------

for i in range(num_displ):
    deslocamento = deslocamento.withColumn(f'pos_inter_{i}', col('pos_inter')[i])

for i in range(num_displ):
    deslocamento = deslocamento.withColumn(f'erb_nearst_inter_{i}', retrieve_batch_queries(f'pos_inter_{i}'))


# COMMAND ----------

display(deslocamento)

# COMMAND ----------

num_sess_per_cgi = num_sess// num_displ

# COMMAND ----------

# DBTITLE 1,Transformando deslocamento para antenas
# Transform deslocamento para:
# msisdn, tx_uplink, tx_downlink, qt_volume, qt_duration, ds_start_time, ds_end_time, nu_served_imei, cd_cgi
deslocamento = deslocamento.withColumn(
    "cd_cgi_percurso", array(*[col(f"erb_nearst_inter_{i}").retrieved_id for i in range(num_displ)])
).withColumn(
    "num_sess", array(*[lit(i) for i in range(num_sess)])
).withColumn(
    "cd_pais", lit(55)
).withColumn(
    "hora_saida", from_unixtime(
        unix_timestamp(lit(data_ref)) + 
                      ((rand() * (arrival_time - departure_time) + departure_time) * 3600)).cast("timestamp")
).select(
    col("nu_tlfn").alias("msisdn"),
    col("nu_imei_aprl").alias("num_served_imei"),
    col("cd_area"),
    col("cd_pais"),
    "cd_cgi_percurso",
    "num_sess",
    "hora_saida"
)
 
display(deslocamento)

# COMMAND ----------

deslocamento.write.mode("overwrite").saveAsTable(f"{catalog_name}.misc.aux_tbl_trajeto")

# COMMAND ----------

deslocamento_cliente = deslocamento.withColumn(
    "sessao", explode(col("num_sess"))
    ).withColumn("cd_cgi",  col("cd_cgi_percurso")[(col("sessao") / num_sess_per_cgi).cast("bigint")]
    ).withColumn("tx_uplink", (rand() * commute_tx_up).cast("int")
    ).withColumn("tx_downlink", (rand() * commute_tx_down).cast("int")           
    ).withColumn("qt_duration", (rand() * 55).cast("int")
    ).withColumn("qt_volume", col("tx_downlink") * col("qt_duration")
    ).withColumn("delta_time_start", (rand() * 29).cast("int")           
    ).withColumn("delta_time_end", (rand() * 30 + 30).cast("int")           
    ).withColumn("ds_start_time", from_unixtime(unix_timestamp(col('hora_saida')) + col("delta_time_start") + (60 *  col("sessao"))).cast("timestamp")
    ).withColumn("ds_end_time", from_unixtime(unix_timestamp(col('ds_start_time')) + col("qt_duration")).cast("timestamp")
    ).drop("num_sess", "cd_cgi_percurso")   

display(deslocamento_cliente)

# COMMAND ----------

deslocamento_cliente= deslocamento_cliente.select(
    "msisdn",
    "tx_uplink",
    "tx_downlink",
    "qt_volume",
    "qt_duration",
    "ds_start_time",
    "ds_end_time",
    "num_served_imei",
    "cd_cgi",
    "cd_area",
    "cd_pais"
)

# COMMAND ----------

display(deslocamento_cliente)

# COMMAND ----------

# DBTITLE 1,Append na tablea au_tbl_antenna_pattern
deslocamento_cliente.write.mode("append").saveAsTable(f"{catalog_name}.misc.aux_tbl_antenna_pattern")
