# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO g0_ops_productos.silver.hm_producto_performance (
# MAGIC   periodo,
# MAGIC   producto_id,
# MAGIC   producto_nombre,
# MAGIC   total_ventas,
# MAGIC   stock_actual,
# MAGIC   rotacion,
# MAGIC   ranking_rotacion,
# MAGIC   inserted_at
# MAGIC )
# MAGIC WITH parametros AS (
# MAGIC   SELECT
# MAGIC     DATE('2024-02-01') AS fecha_actual,
# MAGIC     DATE_FORMAT(ADD_MONTHS(DATE_TRUNC('month', fecha_actual), -1), 'yyyyMM') AS cod_mes
# MAGIC ),
# MAGIC inventario_actual AS (
# MAGIC   SELECT
# MAGIC     i.cod_mes,
# MAGIC     i.producto_id,
# MAGIC     i.producto_nombre,
# MAGIC     i.stock
# MAGIC   FROM g0_ops_productos.gold.vw_inventario i, parametros
# MAGIC   WHERE i.cod_mes = parametros.cod_mes
# MAGIC ),
# MAGIC ventas_mes AS (
# MAGIC   SELECT
# MAGIC     v.cod_mes,
# MAGIC     v.producto_id,
# MAGIC     SUM(v.cantidad) AS total_ventas
# MAGIC   FROM g0_cmc_ventas.gold.vw_ventas v, parametros
# MAGIC   -- WHERE fecha_venta >= DATE_TRUNC('month', parametros.fecha_actual - INTERVAL 1 MONTH)
# MAGIC   WHERE v.cod_mes = parametros.cod_mes
# MAGIC   GROUP BY
# MAGIC     v.cod_mes,
# MAGIC     v.producto_id
# MAGIC ),
# MAGIC ventas_inventario AS (
# MAGIC   SELECT
# MAGIC     i.cod_mes,
# MAGIC     i.producto_id,
# MAGIC     i.producto_nombre,
# MAGIC     COALESCE(v.total_ventas, 0) AS total_ventas,
# MAGIC     i.stock AS stock_actual,
# MAGIC     ROUND(CASE WHEN i.stock > 0 THEN v.total_ventas / i.stock ELSE NULL END, 2) AS rotacion
# MAGIC   FROM inventario_actual i
# MAGIC   LEFT JOIN ventas_mes v ON i.cod_mes = v.cod_mes AND i.producto_id = v.producto_id
# MAGIC )
# MAGIC SELECT
# MAGIC   cod_mes AS periodo,
# MAGIC   producto_id,
# MAGIC   producto_nombre,
# MAGIC   total_ventas,
# MAGIC   stock_actual,
# MAGIC   rotacion,
# MAGIC   RANK() OVER (ORDER BY rotacion DESC) AS ranking_rotacion,
# MAGIC   current_timestamp() AS inserted_at
# MAGIC FROM ventas_inventario;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE g0_ops_productos.silver.hm_producto_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY g0_ops_productos.silver.hm_producto_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW IF EXISTS g0_ops_productos.silver.mv_um_producto_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW g0_ops_productos.silver.mv_um_producto_performance
# MAGIC (
# MAGIC   periodo STRING COMMENT 'Periodo de tiempo mensual en el que se calculó el performance',
# MAGIC   producto_id STRING COMMENT 'Identificador único del producto',
# MAGIC   producto_nombre STRING COMMENT 'Nombre del producto',
# MAGIC   total_ventas INT COMMENT 'Total de unidades vendidas en el periodo',
# MAGIC   stock_actual INT COMMENT 'Stock actual al cierre del periodo',
# MAGIC   rotacion DOUBLE COMMENT 'Ratio de rotación del producto en el periodo calculado como total_ventas / stock_actual.',
# MAGIC   ranking_rotacion INT COMMENT 'Ranking dentro del mes por rotación, orden descendente.',
# MAGIC   inserted_at TIMESTAMP COMMENT 'Timestamp en que se insertó este snapshot'
# MAGIC )
# MAGIC COMMENT 'Tabla con indicadores de performance de productos del último mes disponible'
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
# MAGIC FROM g0_ops_productos.silver.hm_producto_performance
# MAGIC WHERE periodo = (SELECT periodo FROM g0_ops_productos.silver.hm_producto_performance ORDER BY periodo DESC LIMIT 1)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH g0_ops_productos.silver.mv_um_producto_performance