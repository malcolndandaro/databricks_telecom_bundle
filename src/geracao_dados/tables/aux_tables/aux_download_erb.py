# Databricks notebook source
!pip install gitpython==3.1.43

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("p_catalog","dev", "Nome do catálogo")
catalog_name        = dbutils.widgets.get("p_catalog")

# COMMAND ----------
spark.sql(f"CREATE catalog if not exists {catalog_name}")
spark.sql(f"CREATE schema if not exists {catalog_name}.misc")
spark.sql(f"""
CREATE VOLUME if not exists {catalog_name}.misc.aux_files
COMMENT 'Dados de ERBs, csv e pickle'
""")

# COMMAND ----------

# COMMAND ----------
# Download ERB data from repo
import git
import shutil
import os

# Define the repository URL and the directory path within the repository
repo_url = "https://github.com/ctakamiya/sim_telco_dataset/"
local_dir = f"/Volumes/{catalog_name}/misc/aux_files"

# Clone the repository to a temporary directory
temp_dir = "/tmp/repo"

# Ensure the temporary directory does not exist or is empty
if os.path.exists(temp_dir):
    shutil.rmtree(temp_dir)

git.Repo.clone_from(repo_url, temp_dir)


# Copy the specific directory to the desired location in UC



# Copying the csv files
shutil.copytree(
    f"{temp_dir}",
    f"{local_dir}",
    dirs_exist_ok=True,
    ignore=shutil.ignore_patterns(".git")
)


# Clean up the temporary directory
shutil.rmtree(temp_dir)