# Databricks notebook source
# MAGIC %pip install databricks-sdk

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

env = dbutils.widgets.getArgument("env")
print(env)

# COMMAND ----------

# MAGIC %run ../tags/Tags

# COMMAND ----------

base_policy = {
    "autotermination_minutes": {
        "type": "range",
        "defaultValue": 15,
        "minValue": 15,
        "isOptional": False,
    },
}

# COMMAND ----------

for project in tag_list:
    tag_policy = tag_list[project]
    policy = base_policy.copy()
    policy["custom_tags.env"] = { "type": "fixed", "value": env }
    policy["custom_tags.domain"] = { "type": "fixed", "value": tag_policy["domain"] }
    policy["custom_tags.cost_center"] = { "type": "fixed", "value": tag_policy["cost_center"] }
    policy["custom_tags.project"] = { "type": "fixed", "value": project }
    de_cluster_policy = w.cluster_policies.create(
        name=f"Projeto {project}",
        definition=json.dumps(policy),
    )
