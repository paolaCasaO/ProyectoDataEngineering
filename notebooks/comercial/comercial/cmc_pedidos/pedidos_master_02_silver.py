# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views and Materialized Views only needs to be created the first time
# MAGIC - Materialized views only will beed to be REFRESHED (Point to DataPlatform's REFRESH notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE SERVERLESS COMPUTE

# COMMAND ----------


dbutils.widgets.text('SILVER_PEDIDOS_PTA', 'g6_cmc_pedidos.silver.mv_pedidos_pta')

dbutils.widgets.text('BRONZE_TB_RAW_PEDIDOS', 'g6_cmc_pedidos.bronze.raw_pedidos')

# COMMAND ----------

print('SILVER_PEDIDOS_PTA\t\t:', dbutils.widgets.get("SILVER_PEDIDOS_PTA")) 

print('BRONZE_TB_RAW_PEDIDOS\t: ',dbutils.widgets.get("BRONZE_TB_RAW_PEDIDOS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE WAREHOUSE COMPUTE
# MAGIC Materialized Views only run in Warehouse Compute

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW IF EXISTS ${SILVER_PEDIDOS_PTA};
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vista Materializada Silver: Análisis de pedidos por cliente y zonas geográficas
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW ${SILVER_PEDIDOS_PTA}
# MAGIC AS
# MAGIC WITH pedidos_base AS (
# MAGIC   SELECT 
# MAGIC     cust_code,
# MAGIC     no_cli,
# MAGIC     
# MAGIC     -- Información temporal
# MAGIC     CAST(order_date AS DATE) as fecha_pedido,
# MAGIC     DATE_TRUNC('month', order_date) as mes_pedido,
# MAGIC     DATE_TRUNC('quarter', order_date) as trimestre_pedido,
# MAGIC     YEAR(order_date) as ano_pedido,
# MAGIC     DAYOFWEEK(order_date) as dia_semana,
# MAGIC     
# MAGIC     -- Información del pedido
# MAGIC     proj_code,
# MAGIC     order_code,
# MAGIC     co_obr,
# MAGIC     de_obr,
# MAGIC     prod_code,
# MAGIC     short_prod_descr,
# MAGIC     elemento,
# MAGIC     frecuencia,
# MAGIC     tamano_de_carga,
# MAGIC     
# MAGIC     -- Métricas de cantidad
# MAGIC     COALESCE(order_qty, 0) as order_qty,
# MAGIC     COALESCE(delv_qty, 0) as delv_qty,
# MAGIC     CASE WHEN COALESCE(order_qty, 0) > 0 THEN (COALESCE(delv_qty, 0) / order_qty) * 100 ELSE 0 END as cumplimiento_pct,
# MAGIC     
# MAGIC     -- Métricas de tiempo promedio
# MAGIC     COALESCE(promedio_tiempo_a_obra, 0) as tiempo_a_obra_promedio,
# MAGIC     COALESCE(promedio_tiempo_espera, 0) as tiempo_espera_promedio,
# MAGIC     COALESCE(promedio_tiempo_vaciado, 0) as tiempo_vaciado_promedio,
# MAGIC     COALESCE(promedio_tiempo_salida, 0) as tiempo_salida_promedio,
# MAGIC     COALESCE(promedio_tiempo_a_planta, 0) as tiempo_a_planta_promedio,
# MAGIC     (COALESCE(promedio_tiempo_a_obra, 0) + COALESCE(promedio_tiempo_espera, 0) + COALESCE(promedio_tiempo_vaciado, 0) + 
# MAGIC      COALESCE(promedio_tiempo_salida, 0) + COALESCE(promedio_tiempo_a_planta, 0)) as ciclo_total_promedio,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 🗺️ ANÁLISIS GEOGRÁFICO DE ZONAS
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Coordenadas originales
# MAGIC     obra_latitud,
# MAGIC     obra_longitud,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Identificación de zonas geográficas basadas en coordenadas
# MAGIC     -- Definición de zonas para Lima, Perú (ajustar según tu región)
# MAGIC     CASE 
# MAGIC       WHEN obra_latitud IS NULL OR obra_longitud IS NULL THEN 'ZONA_SIN_COORDENADAS'
# MAGIC       
# MAGIC       -- Zona Norte de Lima (-11.8 a -12.0)
# MAGIC       WHEN obra_latitud >= -12.0 AND obra_latitud <= -11.8 
# MAGIC            AND obra_longitud >= -77.2 AND obra_longitud <= -76.8 THEN 'ZONA_NORTE'
# MAGIC       
# MAGIC       -- Zona Centro de Lima (-12.0 a -12.15)  
# MAGIC       WHEN obra_latitud >= -12.15 AND obra_latitud <= -12.0 
# MAGIC            AND obra_longitud >= -77.2 AND obra_longitud <= -76.8 THEN 'ZONA_CENTRO'
# MAGIC       
# MAGIC       -- Zona Sur de Lima (-12.15 a -12.35)
# MAGIC       WHEN obra_latitud >= -12.35 AND obra_latitud <= -12.15 
# MAGIC            AND obra_longitud >= -77.2 AND obra_longitud <= -76.8 THEN 'ZONA_SUR'
# MAGIC       
# MAGIC       -- Zona Este (más hacia la sierra)
# MAGIC       WHEN obra_longitud >= -76.8 AND obra_longitud <= -76.5 THEN 'ZONA_ESTE'
# MAGIC       
# MAGIC       -- Zona Oeste (hacia el océano)
# MAGIC       WHEN obra_longitud >= -77.5 AND obra_longitud <= -77.2 THEN 'ZONA_OESTE'
# MAGIC       
# MAGIC       -- Callao (zona portuaria)
# MAGIC       WHEN obra_latitud >= -12.1 AND obra_latitud <= -11.9 
# MAGIC            AND obra_longitud >= -77.3 AND obra_longitud <= -77.0 THEN 'ZONA_CALLAO'
# MAGIC       
# MAGIC       ELSE 'ZONA_PERIFERICA'
# MAGIC     END as zona_geografica_obra,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Subzonas más específicas (cuadrantes dentro de cada zona)
# MAGIC     CASE 
# MAGIC       WHEN obra_latitud IS NULL OR obra_longitud IS NULL THEN 'SIN_COORDENADAS'
# MAGIC       
# MAGIC       -- División por cuadrantes usando punto central aproximado de Lima (-12.05, -77.05)
# MAGIC       WHEN obra_latitud >= -12.05 AND obra_longitud >= -77.05 THEN 'CUADRANTE_NOROESTE'
# MAGIC       WHEN obra_latitud >= -12.05 AND obra_longitud < -77.05 THEN 'CUADRANTE_NORESTE'  
# MAGIC       WHEN obra_latitud < -12.05 AND obra_longitud >= -77.05 THEN 'CUADRANTE_SUROESTE'
# MAGIC       WHEN obra_latitud < -12.05 AND obra_longitud < -77.05 THEN 'CUADRANTE_SURESTE'
# MAGIC       
# MAGIC       ELSE 'CUADRANTE_EXTERNO'
# MAGIC     END as cuadrante_obra,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Anillos concéntricos desde el centro (distancia aproximada)
# MAGIC     CASE 
# MAGIC       WHEN obra_latitud IS NULL OR obra_longitud IS NULL THEN 'SIN_COORDENADAS'
# MAGIC       ELSE
# MAGIC         CASE 
# MAGIC           WHEN SQRT(POWER((obra_latitud - (-12.05)) * 111, 2) + POWER((obra_longitud - (-77.05)) * 111 * COS(RADIANS(obra_latitud)), 2)) <= 5 THEN 'ANILLO_CENTRO_5KM'
# MAGIC           WHEN SQRT(POWER((obra_latitud - (-12.05)) * 111, 2) + POWER((obra_longitud - (-77.05)) * 111 * COS(RADIANS(obra_latitud)), 2)) <= 10 THEN 'ANILLO_MEDIO_10KM'
# MAGIC           WHEN SQRT(POWER((obra_latitud - (-12.05)) * 111, 2) + POWER((obra_longitud - (-77.05)) * 111 * COS(RADIANS(obra_latitud)), 2)) <= 20 THEN 'ANILLO_EXTERIOR_20KM'
# MAGIC           ELSE 'ANILLO_LEJANO_MAS20KM'
# MAGIC         END
# MAGIC     END as anillo_distancia_obra,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Categorización por densidad urbana (aproximada por coordenadas)
# MAGIC     CASE 
# MAGIC       WHEN obra_latitud IS NULL OR obra_longitud IS NULL THEN 'DENSIDAD_DESCONOCIDA'
# MAGIC       
# MAGIC       -- Centro financiero/comercial (alta densidad)
# MAGIC       WHEN obra_latitud >= -12.08 AND obra_latitud <= -12.02 
# MAGIC            AND obra_longitud >= -77.08 AND obra_longitud <= -77.02 THEN 'DENSIDAD_ALTA_COMERCIAL'
# MAGIC       
# MAGIC       -- Zonas residenciales consolidadas
# MAGIC       WHEN obra_latitud >= -12.2 AND obra_latitud <= -11.9 
# MAGIC            AND obra_longitud >= -77.15 AND obra_longitud <= -76.9 THEN 'DENSIDAD_MEDIA_RESIDENCIAL'
# MAGIC       
# MAGIC       -- Zonas periféricas
# MAGIC       WHEN obra_latitud < -12.3 OR obra_latitud > -11.8 
# MAGIC            OR obra_longitud < -77.3 OR obra_longitud > -76.7 THEN 'DENSIDAD_BAJA_PERIFERICA'
# MAGIC       
# MAGIC       ELSE 'DENSIDAD_MEDIA_MIXTA'
# MAGIC     END as densidad_urbana_obra,
# MAGIC     
# MAGIC     -- Horarios
# MAGIC     start_time,
# MAGIC     file_date
# MAGIC     
# MAGIC   FROM ${BRONZE_TB_RAW_PEDIDOS}
# MAGIC   WHERE cust_code IS NOT NULL 
# MAGIC     AND order_date IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- ========================================
# MAGIC -- 🗺️ MÉTRICAS AGREGADAS POR ZONA GEOGRÁFICA
# MAGIC -- ========================================
# MAGIC metricas_agregadas AS (
# MAGIC   SELECT 
# MAGIC     cust_code,
# MAGIC     no_cli,
# MAGIC     fecha_pedido,
# MAGIC     mes_pedido,
# MAGIC     trimestre_pedido,
# MAGIC     ano_pedido,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Agrupación por zonas geográficas
# MAGIC     zona_geografica_obra,
# MAGIC     cuadrante_obra,
# MAGIC     anillo_distancia_obra,
# MAGIC     densidad_urbana_obra,
# MAGIC     
# MAGIC     -- Contadores básicos
# MAGIC     COUNT(*) as total_pedidos_dia,
# MAGIC     COUNT(DISTINCT order_code) as ordenes_unicas_dia,
# MAGIC     COUNT(DISTINCT co_obr) as obras_dia,
# MAGIC     COUNT(DISTINCT prod_code) as productos_diferentes_dia,
# MAGIC     COUNT(DISTINCT proj_code) as proyectos_diferentes_dia,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Contadores geográficos
# MAGIC     COUNT(DISTINCT zona_geografica_obra) as zonas_diferentes_dia,
# MAGIC     COUNT(DISTINCT cuadrante_obra) as cuadrantes_diferentes_dia,
# MAGIC     
# MAGIC     -- Métricas de volumen diarias
# MAGIC     SUM(order_qty) as volumen_pedido_dia,
# MAGIC     SUM(delv_qty) as volumen_entregado_dia,
# MAGIC     AVG(order_qty) as volumen_promedio_pedido,
# MAGIC     AVG(delv_qty) as volumen_promedio_entregado,
# MAGIC     
# MAGIC     -- Métricas de cumplimiento
# MAGIC     AVG(cumplimiento_pct) as cumplimiento_promedio_dia,
# MAGIC     COUNT(CASE WHEN cumplimiento_pct >= 100 THEN 1 END) as pedidos_cumplidos_completos,
# MAGIC     COUNT(CASE WHEN cumplimiento_pct >= 95 THEN 1 END) as pedidos_casi_completos,
# MAGIC     COUNT(CASE WHEN cumplimiento_pct < 80 THEN 1 END) as pedidos_con_faltante,
# MAGIC     COUNT(CASE WHEN delv_qty = 0 THEN 1 END) as pedidos_no_entregados,
# MAGIC     
# MAGIC     -- Análisis de productos
# MAGIC     ARRAY_JOIN(COLLECT_SET(short_prod_descr), ', ') as productos_utilizados,
# MAGIC     ARRAY_JOIN(COLLECT_SET(CAST(tamano_de_carga AS STRING)), ', ') as tamanos_carga_utilizados,
# MAGIC     ARRAY_JOIN(COLLECT_SET(elemento), ', ') as elementos_utilizados,
# MAGIC     
# MAGIC     -- Métricas de tiempo promedio
# MAGIC     AVG(tiempo_a_obra_promedio) as tiempo_obra_promedio,
# MAGIC     AVG(tiempo_espera_promedio) as tiempo_espera_promedio,
# MAGIC     AVG(tiempo_vaciado_promedio) as tiempo_vaciado_promedio,
# MAGIC     AVG(tiempo_salida_promedio) as tiempo_salida_promedio,
# MAGIC     AVG(tiempo_a_planta_promedio) as tiempo_retorno_promedio,
# MAGIC     AVG(ciclo_total_promedio) as ciclo_total_promedio,
# MAGIC     
# MAGIC     -- Análisis de frecuencia
# MAGIC     AVG(frecuencia) as frecuencia_promedio_dia,
# MAGIC     MIN(frecuencia) as frecuencia_minima,
# MAGIC     MAX(frecuencia) as frecuencia_maxima,
# MAGIC     
# MAGIC     -- ✅ NUEVO: Análisis geográfico detallado
# MAGIC     AVG(obra_latitud) as latitud_promedio_obras,
# MAGIC     AVG(obra_longitud) as longitud_promedio_obras,
# MAGIC     MIN(obra_latitud) as latitud_min_obras,
# MAGIC     MAX(obra_latitud) as latitud_max_obras,
# MAGIC     MIN(obra_longitud) as longitud_min_obras,
# MAGIC     MAX(obra_longitud) as longitud_max_obras,
# MAGIC     
# MAGIC     -- Dispersión geográfica (varianza de coordenadas)
# MAGIC     STDDEV(obra_latitud) as dispersion_latitud,
# MAGIC     STDDEV(obra_longitud) as dispersion_longitud,
# MAGIC     
# MAGIC     -- Análisis de proyectos
# MAGIC     ARRAY_JOIN(COLLECT_SET(CAST(proj_code AS STRING)), ', ') as proyectos_atendidos,
# MAGIC     
# MAGIC     file_date
# MAGIC     
# MAGIC   FROM pedidos_base
# MAGIC   GROUP BY 
# MAGIC     cust_code, no_cli, 
# MAGIC     fecha_pedido, mes_pedido, trimestre_pedido, ano_pedido,
# MAGIC     zona_geografica_obra, cuadrante_obra, anillo_distancia_obra, densidad_urbana_obra,
# MAGIC     file_date
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   cust_code,
# MAGIC   no_cli,
# MAGIC   fecha_pedido,
# MAGIC   mes_pedido,
# MAGIC   trimestre_pedido,
# MAGIC   ano_pedido,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🗺️ INFORMACIÓN GEOGRÁFICA DE ZONAS
# MAGIC   -- ========================================
# MAGIC   zona_geografica_obra,
# MAGIC   cuadrante_obra,
# MAGIC   anillo_distancia_obra,
# MAGIC   densidad_urbana_obra,
# MAGIC   
# MAGIC   -- Métricas básicas del día
# MAGIC   total_pedidos_dia,
# MAGIC   ordenes_unicas_dia,
# MAGIC   obras_dia,
# MAGIC   productos_diferentes_dia,
# MAGIC   proyectos_diferentes_dia,
# MAGIC   zonas_diferentes_dia,
# MAGIC   cuadrantes_diferentes_dia,
# MAGIC   
# MAGIC   -- Volúmenes
# MAGIC   volumen_pedido_dia,
# MAGIC   volumen_entregado_dia,
# MAGIC   ROUND(volumen_promedio_pedido, 2) as volumen_promedio_pedido,
# MAGIC   ROUND(volumen_promedio_entregado, 2) as volumen_promedio_entregado,
# MAGIC   
# MAGIC   -- Cumplimiento
# MAGIC   ROUND(cumplimiento_promedio_dia, 2) as cumplimiento_promedio_dia,
# MAGIC   pedidos_cumplidos_completos,
# MAGIC   pedidos_casi_completos,
# MAGIC   pedidos_con_faltante,
# MAGIC   pedidos_no_entregados,
# MAGIC   ROUND((pedidos_cumplidos_completos * 100.0) / total_pedidos_dia, 2) as tasa_cumplimiento_completo_pct,
# MAGIC   
# MAGIC   -- Eficiencia del cumplimiento
# MAGIC   CASE 
# MAGIC     WHEN volumen_pedido_dia > 0 
# MAGIC     THEN ROUND((volumen_entregado_dia / volumen_pedido_dia) * 100, 2)
# MAGIC     ELSE 0 
# MAGIC   END as eficiencia_entrega_pct,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🗺️ CATEGORIZACIONES GEOGRÁFICAS
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Categorización por concentración geográfica
# MAGIC   CASE 
# MAGIC     WHEN zonas_diferentes_dia = 1 AND cuadrantes_diferentes_dia = 1 THEN 'CONCENTRADO_ZONA_UNICA'
# MAGIC     WHEN zonas_diferentes_dia <= 2 AND cuadrantes_diferentes_dia <= 2 THEN 'CONCENTRADO_ZONAS_CERCANAS'
# MAGIC     WHEN zonas_diferentes_dia <= 3 AND cuadrantes_diferentes_dia <= 4 THEN 'DISTRIBUIDO_ZONAS_MULTIPLES'
# MAGIC     WHEN zonas_diferentes_dia > 3 OR cuadrantes_diferentes_dia > 4 THEN 'DISPERSO_ZONAS_AMPLIAS'
# MAGIC     ELSE 'PATRON_GEOGRAFICO_MIXTO'
# MAGIC   END as patron_concentracion_geografica,
# MAGIC   
# MAGIC   -- Análisis de dispersión geográfica
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(dispersion_latitud, 0) + COALESCE(dispersion_longitud, 0) <= 0.01 THEN 'DISPERSION_MUY_BAJA'
# MAGIC     WHEN COALESCE(dispersion_latitud, 0) + COALESCE(dispersion_longitud, 0) <= 0.05 THEN 'DISPERSION_BAJA'
# MAGIC     WHEN COALESCE(dispersion_latitud, 0) + COALESCE(dispersion_longitud, 0) <= 0.1 THEN 'DISPERSION_MEDIA'
# MAGIC     WHEN COALESCE(dispersion_latitud, 0) + COALESCE(dispersion_longitud, 0) <= 0.2 THEN 'DISPERSION_ALTA'
# MAGIC     ELSE 'DISPERSION_MUY_ALTA'
# MAGIC   END as categoria_dispersion_geografica,
# MAGIC   
# MAGIC   -- Categorización por densidad urbana predominante
# MAGIC   CASE 
# MAGIC     WHEN densidad_urbana_obra = 'DENSIDAD_ALTA_COMERCIAL' THEN 'CLIENTE_ZONA_COMERCIAL'
# MAGIC     WHEN densidad_urbana_obra = 'DENSIDAD_MEDIA_RESIDENCIAL' THEN 'CLIENTE_ZONA_RESIDENCIAL'
# MAGIC     WHEN densidad_urbana_obra = 'DENSIDAD_BAJA_PERIFERICA' THEN 'CLIENTE_ZONA_PERIFERICA'
# MAGIC     ELSE 'CLIENTE_ZONA_MIXTA'
# MAGIC   END as perfil_cliente_por_zona,
# MAGIC   
# MAGIC   -- Categorización por distancia del centro
# MAGIC   CASE 
# MAGIC     WHEN anillo_distancia_obra = 'ANILLO_CENTRO_5KM' THEN 'CLIENTE_CENTRO_CIUDAD'
# MAGIC     WHEN anillo_distancia_obra = 'ANILLO_MEDIO_10KM' THEN 'CLIENTE_ZONA_INTERMEDIA'
# MAGIC     WHEN anillo_distancia_obra = 'ANILLO_EXTERIOR_20KM' THEN 'CLIENTE_ZONA_EXTERIOR'
# MAGIC     ELSE 'CLIENTE_ZONA_LEJANA'
# MAGIC   END as perfil_cliente_por_distancia,
# MAGIC   
# MAGIC   -- Categorización de la actividad del cliente por día
# MAGIC   CASE 
# MAGIC     WHEN total_pedidos_dia >= 10 THEN 'DIA_INTENSIVO'
# MAGIC     WHEN total_pedidos_dia >= 5 THEN 'DIA_ALTO'
# MAGIC     WHEN total_pedidos_dia >= 3 THEN 'DIA_MODERADO'
# MAGIC     WHEN total_pedidos_dia >= 2 THEN 'DIA_BAJO'
# MAGIC     ELSE 'DIA_MINIMAL'
# MAGIC   END as intensidad_dia,
# MAGIC   
# MAGIC   -- Categorización por volumen diario
# MAGIC   CASE 
# MAGIC     WHEN volumen_pedido_dia >= 1000 THEN 'VOLUMEN_ALTO'
# MAGIC     WHEN volumen_pedido_dia >= 500 THEN 'VOLUMEN_MEDIO'
# MAGIC     WHEN volumen_pedido_dia >= 100 THEN 'VOLUMEN_BAJO'
# MAGIC     ELSE 'VOLUMEN_MINIMAL'
# MAGIC   END as categoria_volumen_dia,
# MAGIC   
# MAGIC   -- Diversidad de recursos
# MAGIC   CASE 
# MAGIC     WHEN productos_diferentes_dia >= 5 THEN 'ALTA_DIVERSIDAD_PRODUCTOS'
# MAGIC     WHEN productos_diferentes_dia >= 3 THEN 'MEDIA_DIVERSIDAD_PRODUCTOS'
# MAGIC     WHEN productos_diferentes_dia >= 2 THEN 'BAJA_DIVERSIDAD_PRODUCTOS'
# MAGIC     ELSE 'SIN_DIVERSIDAD_PRODUCTOS'
# MAGIC   END as diversidad_productos,
# MAGIC   
# MAGIC   -- Análisis de productos y recursos
# MAGIC   LEFT(productos_utilizados, 200) as muestra_productos_utilizados,
# MAGIC   tamanos_carga_utilizados,
# MAGIC   elementos_utilizados,
# MAGIC   LEFT(proyectos_atendidos, 100) as muestra_proyectos_atendidos,
# MAGIC   
# MAGIC   -- Métricas de tiempo promedio
# MAGIC   ROUND(tiempo_obra_promedio, 1) as tiempo_obra_promedio,
# MAGIC   ROUND(tiempo_espera_promedio, 1) as tiempo_espera_promedio,
# MAGIC   ROUND(tiempo_vaciado_promedio, 1) as tiempo_vaciado_promedio,
# MAGIC   ROUND(tiempo_salida_promedio, 1) as tiempo_salida_promedio,
# MAGIC   ROUND(tiempo_retorno_promedio, 1) as tiempo_retorno_promedio,
# MAGIC   ROUND(ciclo_total_promedio, 1) as ciclo_total_promedio,
# MAGIC   
# MAGIC   -- Análisis de frecuencia
# MAGIC   ROUND(frecuencia_promedio_dia, 1) as frecuencia_promedio_dia,
# MAGIC   frecuencia_minima,
# MAGIC   frecuencia_maxima,
# MAGIC   
# MAGIC   -- Eficiencia del ciclo promedio
# MAGIC   CASE 
# MAGIC     WHEN ciclo_total_promedio > 0 
# MAGIC     THEN ROUND((tiempo_vaciado_promedio / ciclo_total_promedio) * 100, 2)
# MAGIC     ELSE 0 
# MAGIC   END as eficiencia_ciclo_promedio_pct,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🗺️ MÉTRICAS GEOGRÁFICAS DETALLADAS
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Coordenadas promedio y rangos
# MAGIC   ROUND(latitud_promedio_obras, 6) as latitud_promedio_obras,
# MAGIC   ROUND(longitud_promedio_obras, 6) as longitud_promedio_obras,
# MAGIC   ROUND(latitud_min_obras, 6) as latitud_min_obras,
# MAGIC   ROUND(latitud_max_obras, 6) as latitud_max_obras,
# MAGIC   ROUND(longitud_min_obras, 6) as longitud_min_obras,
# MAGIC   ROUND(longitud_max_obras, 6) as longitud_max_obras,
# MAGIC   
# MAGIC   -- Dispersión geográfica
# MAGIC   ROUND(COALESCE(dispersion_latitud, 0), 6) as dispersion_latitud,
# MAGIC   ROUND(COALESCE(dispersion_longitud, 0), 6) as dispersion_longitud,
# MAGIC   
# MAGIC   -- Área aproximada cubierta (rectángulo envolvente en km²)
# MAGIC   CASE 
# MAGIC     WHEN latitud_max_obras IS NOT NULL AND latitud_min_obras IS NOT NULL 
# MAGIC          AND longitud_max_obras IS NOT NULL AND longitud_min_obras IS NOT NULL
# MAGIC     THEN ROUND(
# MAGIC       ABS(latitud_max_obras - latitud_min_obras) * 111 * 
# MAGIC       ABS(longitud_max_obras - longitud_min_obras) * 111 * 
# MAGIC       COS(RADIANS((latitud_max_obras + latitud_min_obras) / 2)), 2
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END as area_cobertura_aproximada_km2,
# MAGIC   
# MAGIC   -- Distancia máxima entre obras del día (diagonal del rectángulo)
# MAGIC   CASE 
# MAGIC     WHEN latitud_max_obras IS NOT NULL AND latitud_min_obras IS NOT NULL 
# MAGIC          AND longitud_max_obras IS NOT NULL AND longitud_min_obras IS NOT NULL
# MAGIC     THEN ROUND(
# MAGIC       SQRT(
# MAGIC         POWER((latitud_max_obras - latitud_min_obras) * 111, 2) + 
# MAGIC         POWER((longitud_max_obras - longitud_min_obras) * 111 * 
# MAGIC                COS(RADIANS((latitud_max_obras + latitud_min_obras) / 2)), 2)
# MAGIC       ), 2
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END as distancia_maxima_entre_obras_km,
# MAGIC   
# MAGIC   -- Métricas acumuladas para el mes por zona
# MAGIC   SUM(total_pedidos_dia) OVER (
# MAGIC     PARTITION BY cust_code, zona_geografica_obra, mes_pedido 
# MAGIC     ORDER BY fecha_pedido 
# MAGIC     ROWS UNBOUNDED PRECEDING
# MAGIC   ) as pedidos_acumulados_mes_zona,
# MAGIC   
# MAGIC   SUM(volumen_pedido_dia) OVER (
# MAGIC     PARTITION BY cust_code, zona_geografica_obra, mes_pedido 
# MAGIC     ORDER BY fecha_pedido 
# MAGIC     ROWS UNBOUNDED PRECEDING
# MAGIC   ) as volumen_acumulado_mes_zona,
# MAGIC   
# MAGIC   -- Ranking del cliente por zona por día
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY zona_geografica_obra, fecha_pedido 
# MAGIC     ORDER BY volumen_pedido_dia DESC
# MAGIC   ) as ranking_cliente_dia_zona,
# MAGIC   
# MAGIC   -- Participación del cliente en la zona por día
# MAGIC   ROUND(
# MAGIC     volumen_pedido_dia * 100.0 / 
# MAGIC     SUM(volumen_pedido_dia) OVER (PARTITION BY zona_geografica_obra, fecha_pedido), 
# MAGIC     2
# MAGIC   ) as participacion_cliente_zona_dia_pct,
# MAGIC   
# MAGIC   -- Día de la semana
# MAGIC   CASE DAYOFWEEK(fecha_pedido)
# MAGIC     WHEN 1 THEN 'Domingo'
# MAGIC     WHEN 2 THEN 'Lunes'
# MAGIC     WHEN 3 THEN 'Martes'
# MAGIC     WHEN 4 THEN 'Miércoles'
# MAGIC     WHEN 5 THEN 'Jueves'
# MAGIC     WHEN 6 THEN 'Viernes'
# MAGIC     WHEN 7 THEN 'Sábado'
# MAGIC   END as dia_semana_nombre,
# MAGIC   
# MAGIC   -- Indicadores de performance
# MAGIC   CASE 
# MAGIC     WHEN cumplimiento_promedio_dia >= 98 AND total_pedidos_dia >= 3 THEN 'PERFORMANCE_EXCELENTE'
# MAGIC     WHEN cumplimiento_promedio_dia >= 90 AND total_pedidos_dia >= 2 THEN 'PERFORMANCE_BUENA'
# MAGIC     WHEN cumplimiento_promedio_dia >= 80 THEN 'PERFORMANCE_REGULAR'
# MAGIC     WHEN cumplimiento_promedio_dia >= 60 THEN 'PERFORMANCE_DEFICIENTE'
# MAGIC     ELSE 'PERFORMANCE_CRITICA'
# MAGIC   END as categoria_performance,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🚨 ALERTAS GEOGRÁFICAS Y OPERACIONALES
# MAGIC   -- ========================================
# MAGIC   CASE 
# MAGIC     WHEN pedidos_no_entregados > 0 THEN 'ALERTA: Pedidos no entregados'
# MAGIC     WHEN cumplimiento_promedio_dia < 70 THEN 'ALERTA: Bajo cumplimiento'
# MAGIC     WHEN pedidos_con_faltante > total_pedidos_dia * 0.5 THEN 'ALERTA: Muchos faltantes'
# MAGIC     WHEN ciclo_total_promedio > 400 THEN 'ALERTA: Ciclos muy largos'
# MAGIC     WHEN total_pedidos_dia > 20 THEN 'ALERTA: Sobrecarga operativa'
# MAGIC     WHEN eficiencia_ciclo_promedio_pct < 15 THEN 'ALERTA: Baja eficiencia ciclo'
# MAGIC     WHEN distancia_maxima_entre_obras_km > 50 THEN 'ALERTA: Obras muy dispersas geográficamente'
# MAGIC     WHEN area_cobertura_aproximada_km2 > 100 THEN 'ALERTA: Área de cobertura muy amplia'
# MAGIC     WHEN zona_geografica_obra = 'ZONA_SIN_COORDENADAS' THEN 'ALERTA: Faltan coordenadas geográficas'
# MAGIC     ELSE 'OPERACION_NORMAL'
# MAGIC   END as alerta_operacional,
# MAGIC   
# MAGIC   file_date,
# MAGIC   current_timestamp() as created_at
# MAGIC
# MAGIC FROM metricas_agregadas;

# COMMAND ----------

REFRESH MATERIALIZED VIEW ${SILVER_PEDIDOS_PLANTA};
