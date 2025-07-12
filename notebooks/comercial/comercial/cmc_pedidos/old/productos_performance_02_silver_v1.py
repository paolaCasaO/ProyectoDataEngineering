# Databricks notebook source
# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW  IF EXISTS g0_ops_productos.silver.mv_producto_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW g0_ops_productos.silver.mv_producto_performance
# MAGIC (
# MAGIC   periodo STRING COMMENT 'Periodo de tiempo mensual en el que se calculó el performance',
# MAGIC   producto_id STRING COMMENT 'Identificador único del producto',
# MAGIC   producto_nombre STRING COMMENT 'Nombre del producto',
# MAGIC   total_ventas INTEGER COMMENT 'Total acumulado de unidades vendidas del producto.',
# MAGIC   stock_actual INTEGER COMMENT 'Stock disponible más reciente registrado en inventario.',
# MAGIC   rotacion DOUBLE COMMENT 'Ratio de rotación del producto calculado como total_ventas / stock_actual.',
# MAGIC   ranking_rotacion INTEGER COMMENT 'Ranking del producto con base en su rotación, orden descendente.',
# MAGIC   inserted_at TIMESTAMP COMMENT 'Timestamp en que se calculó este snapshot de performance.'
# MAGIC )
# MAGIC COMMENT 'Vista materializada con indicadores de performance de productos'
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.cod_mes AS periodo,
# MAGIC   p.producto_id,
# MAGIC   p.producto_nombre,
# MAGIC   CAST(COALESCE(SUM(v.cantidad), 0) AS INTEGER) AS total_ventas,
# MAGIC   p.stock AS stock_actual,
# MAGIC   ROUND(CASE WHEN p.stock > 0 THEN SUM(v.cantidad) / p.stock ELSE NULL END, 2) AS rotacion,
# MAGIC   RANK() OVER (ORDER BY CASE WHEN p.stock > 0 THEN SUM(v.cantidad) / p.stock ELSE 0 END DESC) AS ranking_rotacion,
# MAGIC   CURRENT_TIMESTAMP() AS inserted_at
# MAGIC FROM g0_ops_productos.gold.vw_inventario p
# MAGIC LEFT JOIN g0_cmc_ventas.gold.vw_ventas v ON p.cod_mes = v.cod_mes AND p.producto_id = v.producto_id
# MAGIC GROUP BY
# MAGIC   p.cod_mes,
# MAGIC   p.producto_id,
# MAGIC   p.producto_nombre,
# MAGIC   p.stock
# MAGIC   ;
# MAGIC