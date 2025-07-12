# Databricks notebook source
BRONZE_TB_RAW_PEDIDOS = 'g6_cmc_pedidos.bronze.raw_pedidos'



# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {BRONZE_TB_RAW_PEDIDOS}')