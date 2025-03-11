# Databricks notebook source
from pathlib import Path

deploy_notebooks = [
    str(path)
    for path in Path("/Workspace/Repos/Production/dp-notebooks/domains").glob(
        "**/deploy"
    )
    if "example_" not in str(path)
]

display(deploy_notebooks)

# COMMAND ----------

for notebook in deploy_notebooks:
    dbutils.notebook.run(str(notebook), timeout_seconds=180)
