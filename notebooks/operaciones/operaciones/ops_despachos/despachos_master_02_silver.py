# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views and Materialized Views only needs to be created the first time
# MAGIC - Materialized views only will beed to be REFRESHED (Point to DataPlatform's REFRESH notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE SERVERLESS COMPUTE

# COMMAND ----------


dbutils.widgets.text('SILVER_MV_DESPACHOS_OTIF', 'g6_ops_despachos.silver.mv_despachos_otif')
dbutils.widgets.text('SILVER_MV_DESPACHOS_PLANTA', 'g6_ops_despachos.silver.mv_despachos_planta')
dbutils.widgets.text('SILVER_MV_DESPACHOS_CICLO', 'g6_ops_despachos.silver.mv_despachos_ciclo')
dbutils.widgets.text('BRONZE_TB_RAW_DESPACHOS', 'g6_ops_despachos.bronze.raw_despachos')

# COMMAND ----------

print('SILVER_MV_DESPACHOS_OTIF\t\t:', dbutils.widgets.get("SILVER_MV_DESPACHOS_OTIF")) 
print('SILVER_MV_DESPACHOS_PLANTA\t\t:', dbutils.widgets.get("SILVER_MV_DESPACHOS_PLANTA"))
print('SILVER_MV_DESPACHOS_CICLO\t\t:', dbutils.widgets.get("SILVER_MV_DESPACHOS_CICLO"))
print('BRONZE_TB_RAW_DESPACHOS\t: ',dbutils.widgets.get("BRONZE_TB_RAW_DESPACHOS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE WAREHOUSE COMPUTE
# MAGIC Materialized Views only run in Warehouse Compute

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${SILVER_MV_DESPACHOS_CICLO};

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW IF EXISTS ${SILVER_MV_DESPACHOS_OTIF};
# MAGIC DROP MATERIALIZED VIEW IF EXISTS ${SILVER_MV_DESPACHOS_PLANTA};
# MAGIC DROP MATERIALIZED VIEW IF EXISTS ${SILVER_MV_DESPACHOS_CICLO};
# MAGIC     

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW  ${SILVER_MV_DESPACHOS_CICLO}
# MAGIC AS
# MAGIC SELECT 
# MAGIC   CONCAT(order_code, '_', truck_code, '_', DATE_FORMAT(start_time, 'yyyyMMddHHmm')) as despacho_id,
# MAGIC   order_code,
# MAGIC   truck_code,
# MAGIC   plant_name,
# MAGIC   CAST(order_date AS DATE) as fecha_despacho,
# MAGIC   start_time,
# MAGIC   on_job_time,
# MAGIC   at_plant_time,
# MAGIC   COALESCE(tiempo_a_obra, 0) as tiempo_a_obra,
# MAGIC   COALESCE(tiempo_espera, 0) as tiempo_espera,
# MAGIC   COALESCE(tiempo_vaciado, 0) as tiempo_vaciado,
# MAGIC   COALESCE(tiempo_salida, 0) as tiempo_salida,
# MAGIC   COALESCE(tiempo_a_planta, 0) as tiempo_a_planta,
# MAGIC   (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) as tiempo_ciclo_total,
# MAGIC   COALESCE(tiempo_vaciado, 0) as tiempo_productivo,
# MAGIC   (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) as tiempo_transporte,
# MAGIC   (COALESCE(tiempo_espera, 0) + COALESCE(tiempo_salida, 0)) as tiempo_no_productivo,
# MAGIC   CASE 
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
# MAGIC     THEN ROUND((COALESCE(tiempo_vaciado, 0) / (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0))) * 100, 2)
# MAGIC     ELSE 0 
# MAGIC   END as eficiencia_ciclo,
# MAGIC   CASE 
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 120 THEN 'Rápido'
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 180 THEN 'Normal'
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 240 THEN 'Lento'
# MAGIC     ELSE 'Muy Lento'
# MAGIC   END as categoria_ciclo,
# MAGIC   ((COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) / 60.0) * 30 as distancia_estimada,
# MAGIC   CASE 
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
# MAGIC     THEN 30.0
# MAGIC     ELSE 0 
# MAGIC   END as velocidad_promedio,
# MAGIC   COALESCE(delv_qty, 0) as delv_qty,
# MAGIC   CASE 
# MAGIC     WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
# MAGIC     THEN COALESCE(delv_qty, 0) / (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0))
# MAGIC     ELSE 0 
# MAGIC   END as productividad,
# MAGIC   file_date,
# MAGIC   current_timestamp() as created_at
# MAGIC FROM ${BRONZE_TB_RAW_DESPACHOS}
# MAGIC WHERE start_time IS NOT NULL 
# MAGIC   AND order_code IS NOT NULL
# MAGIC   AND truck_code IS NOT NULL
# MAGIC   AND plant_name IS NOT NULL;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar datos de muestra
# MAGIC SELECT 
# MAGIC   despacho_id,
# MAGIC   plant_name,
# MAGIC   categoria_ciclo,
# MAGIC   tiempo_ciclo_total,
# MAGIC   eficiencia_ciclo,
# MAGIC   productividad
# MAGIC FROM ${SILVER_MV_DESPACHOS_CICLO}
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW ${SILVER_MV_DESPACHOS_CICLO}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW  g6_ops_despachos.silver.mv_despachos_planta
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- ========================================
# MAGIC   --  IDENTIFICADORES DE LA PLANTA
# MAGIC   -- ========================================
# MAGIC   plant_code,
# MAGIC   plant_name,
# MAGIC   COALESCE(plant_latitud, 0) as plant_latitud,
# MAGIC   COALESCE(plant_longitud, 0) as plant_longitud,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  PERÍODO DE ANÁLISIS
# MAGIC   -- ========================================
# MAGIC   DATE_TRUNC('month', order_date) as periodo_mes,
# MAGIC   YEAR(order_date) as ano,
# MAGIC   MONTH(order_date) as mes,
# MAGIC   DATE(order_date) as fecha_analisis,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  CONTADORES Y VOLÚMENES
# MAGIC   -- ========================================
# MAGIC   COUNT(*) as total_despachos,
# MAGIC   COUNT(DISTINCT order_code) as total_ordenes_unicas,
# MAGIC   COUNT(DISTINCT truck_code) as total_camiones_utilizados,
# MAGIC   COUNT(DISTINCT cust_code) as total_clientes_atendidos,
# MAGIC   COUNT(DISTINCT co_obr) as total_obras_atendidas,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  ANÁLISIS DE CANTIDADES
# MAGIC   -- ========================================
# MAGIC   SUM(COALESCE(delv_qty, 0)) as volumen_total_entregado,
# MAGIC   AVG(COALESCE(delv_qty, 0)) as volumen_promedio_por_despacho,
# MAGIC   MIN(COALESCE(delv_qty, 0)) as volumen_minimo,
# MAGIC   MAX(COALESCE(delv_qty, 0)) as volumen_maximo,
# MAGIC   STDDEV(COALESCE(delv_qty, 0)) as volumen_desviacion_estandar,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  ANÁLISIS DE TIEMPOS (en minutos)
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Tiempos promedio
# MAGIC   AVG(COALESCE(tiempo_a_obra, 0)) as tiempo_promedio_a_obra,
# MAGIC   AVG(COALESCE(tiempo_espera, 0)) as tiempo_promedio_espera,
# MAGIC   AVG(COALESCE(tiempo_vaciado, 0)) as tiempo_promedio_vaciado,
# MAGIC   AVG(COALESCE(tiempo_salida, 0)) as tiempo_promedio_salida,
# MAGIC   AVG(COALESCE(tiempo_a_planta, 0)) as tiempo_promedio_a_planta,
# MAGIC   
# MAGIC   -- Tiempo total del ciclo
# MAGIC   AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC       COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC       COALESCE(tiempo_a_planta, 0)) as tiempo_promedio_ciclo_total,
# MAGIC       
# MAGIC   -- Tiempos mínimos y máximos
# MAGIC   MIN(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC       COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC       COALESCE(tiempo_a_planta, 0)) as tiempo_minimo_ciclo,
# MAGIC       
# MAGIC   MAX(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC       COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC       COALESCE(tiempo_a_planta, 0)) as tiempo_maximo_ciclo,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  ANÁLISIS DE TRANSPORTE
# MAGIC   -- ========================================
# MAGIC   AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) as tiempo_promedio_transporte,
# MAGIC   AVG(COALESCE(tiempo_espera, 0) + COALESCE(tiempo_salida, 0)) as tiempo_promedio_no_productivo,
# MAGIC   AVG(COALESCE(tiempo_vaciado, 0)) as tiempo_promedio_productivo,
# MAGIC   
# MAGIC   -- Distancia estimada (asumiendo 30 km/h promedio)
# MAGIC   AVG(((COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) / 60.0) * 30) as distancia_promedio_estimada,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  ANÁLISIS DE EFICIENCIA
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Eficiencia promedio del ciclo (tiempo productivo / tiempo total)
# MAGIC   CASE 
# MAGIC     WHEN AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC               COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC               COALESCE(tiempo_a_planta, 0)) > 0
# MAGIC     THEN ROUND((AVG(COALESCE(tiempo_vaciado, 0)) / 
# MAGIC                 AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                     COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                     COALESCE(tiempo_a_planta, 0))) * 100, 2)
# MAGIC     ELSE 0
# MAGIC   END as eficiencia_promedio_ciclo,
# MAGIC   
# MAGIC   -- Productividad (volumen por minuto de ciclo)
# MAGIC   CASE 
# MAGIC     WHEN AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC               COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC               COALESCE(tiempo_a_planta, 0)) > 0
# MAGIC     THEN ROUND(AVG(COALESCE(delv_qty, 0)) / 
# MAGIC                AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                    COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                    COALESCE(tiempo_a_planta, 0)), 4)
# MAGIC     ELSE 0
# MAGIC   END as productividad_promedio,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- INDICADORES DE RENDIMIENTO
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Porcentaje de despachos por categoría de velocidad
# MAGIC   ROUND((COUNT(CASE WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                          COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                          COALESCE(tiempo_a_planta, 0)) <= 120 THEN 1 END) * 100.0 / COUNT(*)), 2) as porcentaje_despachos_rapidos,
# MAGIC                          
# MAGIC   ROUND((COUNT(CASE WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                          COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                          COALESCE(tiempo_a_planta, 0)) BETWEEN 121 AND 180 THEN 1 END) * 100.0 / COUNT(*)), 2) as porcentaje_despachos_normales,
# MAGIC                          
# MAGIC   ROUND((COUNT(CASE WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                          COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                          COALESCE(tiempo_a_planta, 0)) BETWEEN 181 AND 240 THEN 1 END) * 100.0 / COUNT(*)), 2) as porcentaje_despachos_lentos,
# MAGIC                          
# MAGIC   ROUND((COUNT(CASE WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC                          COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC                          COALESCE(tiempo_a_planta, 0)) > 240 THEN 1 END) * 100.0 / COUNT(*)), 2) as porcentaje_despachos_muy_lentos,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  ANÁLISIS TEMPORAL
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Distribución por horas del día
# MAGIC   ROUND(AVG(HOUR(start_time)), 1) as hora_promedio_inicio,
# MAGIC   MIN(HOUR(start_time)) as hora_inicio_mas_temprana,
# MAGIC   MAX(HOUR(start_time)) as hora_inicio_mas_tardia,
# MAGIC   
# MAGIC   -- Días de la semana más activos
# MAGIC   MODE(DAYOFWEEK(order_date)) as dia_semana_mas_frecuente,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  CLASIFICACIÓN DE LA PLANTA
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC               COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC               COALESCE(tiempo_a_planta, 0)) <= 150 THEN 'Planta Eficiente'
# MAGIC     WHEN AVG(COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + 
# MAGIC               COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + 
# MAGIC               COALESCE(tiempo_a_planta, 0)) <= 200 THEN 'Planta Normal'
# MAGIC     ELSE 'Planta a Mejorar'
# MAGIC   END as clasificacion_eficiencia,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) >= 100 THEN 'Alto Volumen'
# MAGIC     WHEN COUNT(*) >= 50 THEN 'Volumen Medio'
# MAGIC     ELSE 'Bajo Volumen'
# MAGIC   END as clasificacion_volumen,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  TENDENCIAS Y COMPARACIONES
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Comparación con el promedio general (se calculará en una subconsulta)
# MAGIC   AVG(COALESCE(delv_qty, 0)) - 
# MAGIC     (SELECT AVG(COALESCE(delv_qty, 0)) FROM g6_ops_despachos.bronze.raw_despachos) as diferencia_volumen_vs_promedio_general,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   --  METADATOS
# MAGIC   -- ========================================
# MAGIC   MIN(file_date) as fecha_primer_registro,
# MAGIC   MAX(file_date) as fecha_ultimo_registro,
# MAGIC   COUNT(DISTINCT file_date) as dias_con_actividad,
# MAGIC   current_timestamp() as fecha_calculo
# MAGIC
# MAGIC FROM g6_ops_despachos.bronze.raw_despachos
# MAGIC WHERE 
# MAGIC   -- Filtros de calidad de datos
# MAGIC   plant_name IS NOT NULL 
# MAGIC   AND plant_code IS NOT NULL
# MAGIC   AND order_date IS NOT NULL
# MAGIC   AND start_time IS NOT NULL
# MAGIC   
# MAGIC GROUP BY 
# MAGIC   plant_code,
# MAGIC   plant_name,
# MAGIC   plant_latitud,
# MAGIC   plant_longitud,
# MAGIC   DATE_TRUNC('month', order_date),
# MAGIC   YEAR(order_date),
# MAGIC   MONTH(order_date),
# MAGIC   DATE(order_date)
# MAGIC
# MAGIC -- Ordenar por planta y fecha
# MAGIC ORDER BY 
# MAGIC   plant_name,
# MAGIC   fecha_analisis DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver datos de muestra por planta
# MAGIC SELECT 
# MAGIC   plant_name,
# MAGIC   fecha_analisis,
# MAGIC   total_despachos,
# MAGIC   total_ordenes,
# MAGIC   ROUND(volumen_total, 2) as volumen_total,
# MAGIC   ROUND(tasa_otif, 2) as tasa_otif_pct,
# MAGIC   ROUND(tiempo_ciclo_promedio, 1) as tiempo_ciclo_min
# MAGIC FROM ${SILVER_MV_DESPACHOS_PLANTA}
# MAGIC WHERE fecha_analisis >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY fecha_analisis DESC, total_despachos DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear vista materializada de análisis OTIF por orden
# MAGIC CREATE MATERIALIZED VIEW ${SILVER_MV_DESPACHOS_OTIF}
# MAGIC AS
# MAGIC SELECT 
# MAGIC   order_code,
# MAGIC   CAST(order_date AS DATE) as order_date,
# MAGIC   cust_code,
# MAGIC   de_obr,
# MAGIC   plant_name,
# MAGIC   SUM(order_qty) as order_qty,
# MAGIC   SUM(delv_qty) as delv_qty,
# MAGIC   COUNT(*) as total_despachos,
# MAGIC   SUM(CASE WHEN delv_qty >= order_qty THEN 1 ELSE 0 END) as despachos_completos,
# MAGIC   MIN(start_time) as planned_delivery_time,
# MAGIC   MAX(on_job_time) as actual_delivery_time,
# MAGIC   (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 as delivery_delay_minutes,
# MAGIC   ROUND((SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) * 100, 2) as qty_fill_rate,
# MAGIC   CASE WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 THEN true ELSE false END as is_on_time,
# MAGIC   CASE WHEN (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN true ELSE false END as is_in_full,
# MAGIC   CASE 
# MAGIC     WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
# MAGIC          AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN true 
# MAGIC     ELSE false 
# MAGIC   END as is_otif,
# MAGIC   CASE 
# MAGIC     WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
# MAGIC          AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN 'Perfect'
# MAGIC     WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 > 30 
# MAGIC          AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN 'Late'
# MAGIC     WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
# MAGIC          AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) < 0.95 THEN 'Short'
# MAGIC     ELSE 'Late&Short'
# MAGIC   END as otif_category,
# MAGIC   file_date,
# MAGIC   current_timestamp() as created_at
# MAGIC FROM ${BRONZE_TB_RAW_DESPACHOS}
# MAGIC WHERE order_code IS NOT NULL
# MAGIC   AND order_date IS NOT NULL
# MAGIC GROUP BY order_code, CAST(order_date AS DATE), cust_code, de_obr, plant_name, file_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Análisis de órdenes por categoría OTIF
# MAGIC SELECT 
# MAGIC   otif_category,
# MAGIC   COUNT(*) as total_ordenes,
# MAGIC   ROUND(AVG(qty_fill_rate), 2) as fill_rate_promedio,
# MAGIC   ROUND(AVG(delivery_delay_minutes), 1) as delay_promedio_min,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje_total
# MAGIC FROM ${SILVER_MV_DESPACHOS_OTIF}
# MAGIC GROUP BY otif_category
# MAGIC ORDER BY total_ordenes DESC;