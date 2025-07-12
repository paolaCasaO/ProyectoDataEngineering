# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views only needs to be created the first time

# COMMAND ----------


dbutils.widgets.text('GOLD_VW_PUNTUALIDAD_CLIENTE', 'g6_ops_despachos.gold.vw_puntualidad_cliente')

dbutils.widgets.text('SILVER_MV_DESPACHOS_OTIF', 'g6_ops_despachos.silver.mv_despachos_otif')


# COMMAND ----------

SILVER_MV_DESPACHOS_OTIF = dbutils.widgets.get("SILVER_MV_DESPACHOS_OTIF") 
GOLD_VW_PUNTUALIDAD_CLIENTE = dbutils.widgets.get("GOLD_VW_PUNTUALIDAD_CLIENTE")


# COMMAND ----------

print('SILVER_MV_DESPACHOS_OTIF\t:', SILVER_MV_DESPACHOS_OTIF)
print('GOLD_VW_PUNTUALIDAD_CLIENTE\t:', GOLD_VW_PUNTUALIDAD_CLIENTE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vista GOLD: Análisis de puntualidad por cliente
# MAGIC CREATE OR REPLACE VIEW ${GOLD_VW_PUNTUALIDAD_CLIENTE}
# MAGIC COMMENT 'Vista GOLD de análisis de puntualidad y OTIF por cliente'
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- Identificación del cliente
# MAGIC   cust_code,
# MAGIC   COUNT(DISTINCT de_obr) as obras_atendidas,
# MAGIC   COUNT(DISTINCT plant_name) as plantas_utilizadas,
# MAGIC   
# MAGIC   -- Métricas de volumen
# MAGIC   COUNT(*) as total_ordenes,
# MAGIC   SUM(total_despachos) as total_despachos,
# MAGIC   SUM(order_qty) as volumen_ordenado_total,
# MAGIC   SUM(delv_qty) as volumen_entregado_total,
# MAGIC   
# MAGIC   -- Métricas de puntualidad (On Time)
# MAGIC   SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as ordenes_puntuales,
# MAGIC   ROUND(AVG(CASE WHEN is_on_time THEN 100.0 ELSE 0.0 END), 2) as tasa_puntualidad_pct,
# MAGIC   ROUND(AVG(delivery_delay_minutes), 1) as delay_promedio_minutos,
# MAGIC   ROUND(STDDEV(delivery_delay_minutes), 1) as delay_desviacion_std,
# MAGIC   
# MAGIC   -- Métricas de cantidad (In Full)
# MAGIC   SUM(CASE WHEN is_in_full THEN 1 ELSE 0 END) as ordenes_completas,
# MAGIC   ROUND(AVG(CASE WHEN is_in_full THEN 100.0 ELSE 0.0 END), 2) as tasa_completitud_pct,
# MAGIC   ROUND(AVG(qty_fill_rate), 2) as fill_rate_promedio_pct,
# MAGIC   
# MAGIC   -- Métricas OTIF (On Time In Full)
# MAGIC   SUM(CASE WHEN is_otif THEN 1 ELSE 0 END) as ordenes_perfectas,
# MAGIC   ROUND(AVG(CASE WHEN is_otif THEN 100.0 ELSE 0.0 END), 2) as tasa_otif_pct,
# MAGIC   
# MAGIC   -- Distribución por categoría OTIF
# MAGIC   SUM(CASE WHEN otif_category = 'Perfect' THEN 1 ELSE 0 END) as ordenes_perfect,
# MAGIC   SUM(CASE WHEN otif_category = 'Late' THEN 1 ELSE 0 END) as ordenes_late,
# MAGIC   SUM(CASE WHEN otif_category = 'Short' THEN 1 ELSE 0 END) as ordenes_short,
# MAGIC   SUM(CASE WHEN otif_category = 'Late&Short' THEN 1 ELSE 0 END) as ordenes_late_short,
# MAGIC   
# MAGIC   -- Clasificación del cliente por performance
# MAGIC   CASE 
# MAGIC     WHEN AVG(CASE WHEN is_otif THEN 100.0 ELSE 0.0 END) >= 95 THEN 'EXCELENTE'
# MAGIC     WHEN AVG(CASE WHEN is_otif THEN 100.0 ELSE 0.0 END) >= 85 THEN 'BUENO'
# MAGIC     WHEN AVG(CASE WHEN is_otif THEN 100.0 ELSE 0.0 END) >= 70 THEN 'REGULAR'
# MAGIC     ELSE 'NECESITA_MEJORA'
# MAGIC   END as categoria_cliente,
# MAGIC   
# MAGIC   -- Información temporal
# MAGIC   MIN(order_date) as primera_orden,
# MAGIC   MAX(order_date) as ultima_orden,
# MAGIC   DATEDIFF(MAX(order_date), MIN(order_date)) + 1 as dias_como_cliente,
# MAGIC   ROUND(COUNT(*) / (DATEDIFF(MAX(order_date), MIN(order_date)) + 1.0), 2) as ordenes_por_dia,
# MAGIC   
# MAGIC   -- Métricas de tendencia (últimos 30 días vs histórico)
# MAGIC   ROUND(
# MAGIC     AVG(CASE 
# MAGIC       WHEN order_date >= current_date() - INTERVAL 30 DAYS AND is_otif 
# MAGIC       THEN 100.0 ELSE 0.0 
# MAGIC     END), 2
# MAGIC   ) as otif_ultimos_30_dias,
# MAGIC   
# MAGIC   -- Plantas más utilizadas
# MAGIC   FIRST_VALUE(plant_name) OVER (
# MAGIC     PARTITION BY cust_code 
# MAGIC     ORDER BY COUNT(*) DESC
# MAGIC   ) as planta_principal,
# MAGIC   
# MAGIC   -- Timestamp de actualización
# MAGIC   current_timestamp() as updated_at
# MAGIC
# MAGIC FROM ${SILVER_MV_DESPACHOS_OTIF}
# MAGIC WHERE order_date >= current_date() - INTERVAL 365 DAYS  -- Último año
# MAGIC GROUP BY cust_code, plant_name