# Databricks notebook source
# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS g0_ops_productos.gold.vw_producto_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g0_ops_productos.gold.vw_producto_performance
# MAGIC COMMENT 'Vista que contiene el performance histórico de cada producto'
# MAGIC AS
# MAGIC SELECT
# MAGIC   periodo,
# MAGIC   producto_id,
# MAGIC   producto_nombre,
# MAGIC   total_ventas,
# MAGIC   stock_actual,
# MAGIC   rotacion,
# MAGIC   ranking_rotacion,
# MAGIC   inserted_at
# MAGIC FROM g0_ops_productos.silver.mv_producto_performance