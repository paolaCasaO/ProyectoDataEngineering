# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook **will run** in Pipeline
# MAGIC - Need to create temporary views and insert into fixed raw table
# MAGIC - COMMENT widget creation: dbutils.widgets.text() -> It will be created in the pipeline

# COMMAND ----------

from datetime import datetime, date

# COMMAND ----------

dbutils.widgets.text('BRONZE_TB_RAW_CLIENTES', 'g6_mkt_clientes.bronze.raw_clientes')

# COMMAND ----------

BRONZE_TB_RAW_CLIENTES = dbutils.widgets.get("BRONZE_TB_RAW_CLIENTES")

# COMMAND ----------

print('BRONZE_TB_RAW_CLIENTES\t:', BRONZE_TB_RAW_CLIENTES)

# COMMAND ----------

spark.sql(f"""
    DELETE FROM {BRONZE_TB_RAW_CLIENTES}
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE g6_mkt_clientes.bronze.raw_clientes
# MAGIC AS
# MAGIC SELECT *, current_timestamp() AS inserted_at
# MAGIC FROM read_files(
# MAGIC     'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/g06/clientes/',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     inferSchema => true
# MAGIC     --schema => 'id INT, producto STRING, cantidad INT, precio DOUBLE'
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g6_mkt_clientes.bronze.raw_clientes