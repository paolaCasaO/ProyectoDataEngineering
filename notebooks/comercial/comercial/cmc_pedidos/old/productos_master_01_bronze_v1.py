# Databricks notebook source
spark.sql('DROP TABLE IF EXISTS g0_ops_productos.bronze.raw_inventario;')

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE TABLE g0_ops_productos.bronze.raw_inventario
          AS
          SELECT *, current_timestamp() AS inserted_at FROM read_files(
              'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G0/productos/inventario/',
              format => 'csv',
              header => true,
              inferSchema => true
              --schema => 'id INT, producto STRING, cantidad INT, precio DOUBLE'
              )
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g0_ops_productos.bronze.raw_inventario