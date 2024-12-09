# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"])

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

w = WorkspaceClient()

# COMMAND ----------

env = dbutils.widgets.getArgument("env")
print(env)

# COMMAND ----------

# MAGIC %run ../tags/Tags

# COMMAND ----------

def criar_warehouse(project, cluster_size="2X-Small", max_num_clusters=1):
    tag_policy = tag_list[project]
    w.warehouses.create(
        name=f"Warehouse {project}",
        cluster_size=cluster_size,
        max_num_clusters=max_num_clusters,
        auto_stop_mins=10,
        enable_serverless_compute=True, 
        tags=sql.EndpointTags(custom_tags=[
            sql.EndpointTagPair(key="env", value=env),
            sql.EndpointTagPair(key="domain", value=tag_policy["domain"]),
            sql.EndpointTagPair(key="const_center", value=tag_policy["cost_center"]),
            sql.EndpointTagPair(key="project", value=project),
        ])
    )

# COMMAND ----------

if env == "prod":
    criar_warehouse("geral", "Small", 5)
    criar_warehouse("billing", "Small", 5)
    criar_warehouse("crm", "Small", 5)
    criar_warehouse("financeiro", "Small", 5)
    criar_warehouse("microssegmentacao", "Small", 5) 
    criar_warehouse("telecom", "Large", 5)
    criar_warehouse("marketing", "Small", 5) 
else:
    criar_warehouse("geral", "X-Small", 5)
    criar_warehouse("billing", "X-Small", 5)
    criar_warehouse("crm", "X-Small", 5)
    criar_warehouse("financeiro", "X-Small", 5)
    criar_warehouse("microssegmentacao", "X-Small", 5) 
    criar_warehouse("telecom", "X-Small", 5)
    criar_warehouse("marketing", "X-Small", 5) 

