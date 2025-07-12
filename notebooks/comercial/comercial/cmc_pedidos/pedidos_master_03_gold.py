# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views only needs to be created the first time

# COMMAND ----------


dbutils.widgets.text('GOLD_DEMANDA_PEDIDOS', 'g6_cmc_pedidos.gold.vw_demanda_pedidos')

dbutils.widgets.text('SILVER_PEDIDOS_PTA', 'g6_cmc_pedidos.silver.mv_pedidos_pta')


# COMMAND ----------

SILVER_PEDIDOS_PTA = dbutils.widgets.get("SILVER_PEDIDOS_PTA")
GOLD_DEMANDA_PEDIDOS = dbutils.widgets.get("GOLD_DEMANDA_PEDIDOS") 


# COMMAND ----------

print('SILVER_PEDIDOS_PTA\t:', SILVER_PEDIDOS_PTA)
print('GOLD_DEMANDA_PEDIDOS\t:', GOLD_DEMANDA_PEDIDOS)

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${GOLD_DEMANDA_PEDIDOS}
# MAGIC AS
# MAGIC WITH demanda_mensual_cliente AS (
# MAGIC   SELECT 
# MAGIC     cust_code,
# MAGIC     no_cli,
# MAGIC     ano_pedido,
# MAGIC     MONTH(mes_pedido) as mes_numero,
# MAGIC     CONCAT(ano_pedido, '-', LPAD(MONTH(mes_pedido), 2, '0')) as periodo_mes,
# MAGIC     QUARTER(mes_pedido) as trimestre_numero,
# MAGIC     CONCAT(ano_pedido, '-Q', QUARTER(mes_pedido)) as periodo_trimestre,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📊 MÉTRICAS DE DEMANDA MENSUAL
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Contadores básicos
# MAGIC     COUNT(DISTINCT fecha_pedido) as dias_activos_mes,
# MAGIC     SUM(total_pedidos_dia) as total_pedidos_mes,
# MAGIC     SUM(ordenes_unicas_dia) as total_ordenes_mes,
# MAGIC     SUM(obras_dia) as total_obras_mes,
# MAGIC     SUM(productos_diferentes_dia) as total_productos_mes,
# MAGIC     SUM(proyectos_diferentes_dia) as total_proyectos_mes,
# MAGIC     
# MAGIC     -- Volúmenes
# MAGIC     SUM(volumen_pedido_dia) as volumen_pedido_mes,
# MAGIC     SUM(volumen_entregado_dia) as volumen_entregado_mes,
# MAGIC     AVG(volumen_promedio_pedido) as volumen_promedio_pedido_mes,
# MAGIC     AVG(volumen_promedio_entregado) as volumen_promedio_entregado_mes,
# MAGIC     
# MAGIC     -- Cumplimiento
# MAGIC     AVG(cumplimiento_promedio_dia) as cumplimiento_promedio_mes,
# MAGIC     SUM(pedidos_cumplidos_completos) as pedidos_cumplidos_mes,
# MAGIC     SUM(pedidos_casi_completos) as pedidos_casi_completos_mes,
# MAGIC     SUM(pedidos_con_faltante) as pedidos_con_faltante_mes,
# MAGIC     SUM(pedidos_no_entregados) as pedidos_no_entregados_mes,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 🗺️ ANÁLISIS GEOGRÁFICO CONSOLIDADO
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Diversidad geográfica
# MAGIC     COUNT(DISTINCT zona_geografica_obra) as zonas_diferentes_mes,
# MAGIC     COUNT(DISTINCT cuadrante_obra) as cuadrantes_diferentes_mes,
# MAGIC     COUNT(DISTINCT anillo_distancia_obra) as anillos_diferentes_mes,
# MAGIC     COUNT(DISTINCT densidad_urbana_obra) as densidades_diferentes_mes,
# MAGIC     
# MAGIC     -- Zona predominante (moda)
# MAGIC     MODE(zona_geografica_obra) as zona_predominante_mes,
# MAGIC     MODE(cuadrante_obra) as cuadrante_predominante_mes,
# MAGIC     MODE(anillo_distancia_obra) as anillo_predominante_mes,
# MAGIC     MODE(densidad_urbana_obra) as densidad_predominante_mes,
# MAGIC     MODE(perfil_cliente_por_zona) as perfil_zona_predominante_mes,
# MAGIC     MODE(perfil_cliente_por_distancia) as perfil_distancia_predominante_mes,
# MAGIC     
# MAGIC     -- Concentración geográfica
# MAGIC     MODE(patron_concentracion_geografica) as patron_concentracion_predominante,
# MAGIC     MODE(categoria_dispersion_geografica) as dispersion_predominante,
# MAGIC     
# MAGIC     -- Métricas geográficas promedio
# MAGIC     AVG(area_cobertura_aproximada_km2) as area_promedio_cobertura_mes,
# MAGIC     AVG(distancia_maxima_entre_obras_km) as distancia_promedio_maxima_mes,
# MAGIC     MAX(area_cobertura_aproximada_km2) as area_maxima_cobertura_mes,
# MAGIC     MAX(distancia_maxima_entre_obras_km) as distancia_maxima_obras_mes,
# MAGIC     
# MAGIC     -- Coordenadas promedio del cliente
# MAGIC     AVG(latitud_promedio_obras) as latitud_centro_gravedad_mes,
# MAGIC     AVG(longitud_promedio_obras) as longitud_centro_gravedad_mes,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- ⏱️ MÉTRICAS DE TIEMPO Y EFICIENCIA
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     AVG(tiempo_obra_promedio) as tiempo_obra_promedio_mes,
# MAGIC     AVG(tiempo_espera_promedio) as tiempo_espera_promedio_mes,
# MAGIC     AVG(tiempo_vaciado_promedio) as tiempo_vaciado_promedio_mes,
# MAGIC     AVG(ciclo_total_promedio) as ciclo_total_promedio_mes,
# MAGIC     AVG(eficiencia_ciclo_promedio_pct) as eficiencia_promedio_mes,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📈 PATRONES DE DEMANDA
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Intensidad promedio
# MAGIC     MODE(intensidad_dia) as intensidad_predominante_mes,
# MAGIC     MODE(categoria_volumen_dia) as categoria_volumen_predominante_mes,
# MAGIC     MODE(categoria_performance) as performance_predominante_mes,
# MAGIC     
# MAGIC     -- Diversidad de productos
# MAGIC     MODE(diversidad_productos) as diversidad_productos_predominante,
# MAGIC     
# MAGIC     -- Días de la semana más activos
# MAGIC     MODE(dia_semana_nombre) as dia_semana_mas_activo,
# MAGIC     
# MAGIC     -- Alertas
# MAGIC     COUNT(CASE WHEN alerta_operacional != 'OPERACION_NORMAL' THEN 1 END) as dias_con_alertas_mes,
# MAGIC     MODE(alerta_operacional) as tipo_alerta_predominante,
# MAGIC     
# MAGIC     -- Fechas límite
# MAGIC     MIN(fecha_pedido) as primera_fecha_actividad_mes,
# MAGIC     MAX(fecha_pedido) as ultima_fecha_actividad_mes
# MAGIC     
# MAGIC   FROM ${SILVER_PEDIDOS_PTA}
# MAGIC   GROUP BY 
# MAGIC     cust_code, no_cli, ano_pedido, MONTH(mes_pedido), QUARTER(mes_pedido)
# MAGIC ),
# MAGIC
# MAGIC -- ========================================
# MAGIC -- 📊 CÁLCULOS DE MÉTRICAS DERIVADAS
# MAGIC -- ========================================
# MAGIC demanda_con_metricas AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📊 INTENSIDAD Y FRECUENCIA DE DEMANDA
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Intensidad diaria promedio
# MAGIC     ROUND(total_pedidos_mes / GREATEST(dias_activos_mes, 1), 2) as pedidos_promedio_por_dia_activo,
# MAGIC     ROUND(volumen_pedido_mes / GREATEST(dias_activos_mes, 1), 2) as volumen_promedio_por_dia_activo,
# MAGIC     ROUND(total_obras_mes / GREATEST(dias_activos_mes, 1), 2) as obras_promedio_por_dia_activo,
# MAGIC     
# MAGIC     -- Frecuencia de actividad
# MAGIC     ROUND((dias_activos_mes * 100.0) / 
# MAGIC           (CASE 
# MAGIC             WHEN mes_numero IN (1,3,5,7,8,10,12) THEN 31
# MAGIC             WHEN mes_numero IN (4,6,9,11) THEN 30
# MAGIC             WHEN mes_numero = 2 THEN 28
# MAGIC             ELSE 30
# MAGIC           END), 2) as frecuencia_actividad_mes_pct,
# MAGIC     
# MAGIC     -- Eficiencia de entrega
# MAGIC     CASE 
# MAGIC       WHEN volumen_pedido_mes > 0 
# MAGIC       THEN ROUND((volumen_entregado_mes / volumen_pedido_mes) * 100, 2)
# MAGIC       ELSE 0 
# MAGIC     END as eficiencia_entrega_mes_pct,
# MAGIC     
# MAGIC     -- Tasa de cumplimiento completo
# MAGIC     CASE 
# MAGIC       WHEN total_pedidos_mes > 0 
# MAGIC       THEN ROUND((pedidos_cumplidos_mes * 100.0) / total_pedidos_mes, 2)
# MAGIC       ELSE 0 
# MAGIC     END as tasa_cumplimiento_completo_mes_pct
# MAGIC     
# MAGIC   FROM demanda_mensual_cliente
# MAGIC ),
# MAGIC
# MAGIC -- ========================================
# MAGIC -- 📈 CÁLCULO DE TENDENCIAS Y COMPARACIONES
# MAGIC -- ========================================
# MAGIC demanda_con_tendencias AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📈 COMPARACIONES TEMPORALES
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Comparación mes anterior
# MAGIC     LAG(total_pedidos_mes) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC     ) as pedidos_mes_anterior,
# MAGIC     
# MAGIC     LAG(volumen_pedido_mes) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC     ) as volumen_mes_anterior,
# MAGIC     
# MAGIC     LAG(eficiencia_entrega_mes_pct) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC     ) as eficiencia_mes_anterior,
# MAGIC     
# MAGIC     -- Comparación mismo mes año anterior
# MAGIC     LAG(total_pedidos_mes, 12) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC     ) as pedidos_mismo_mes_ano_anterior,
# MAGIC     
# MAGIC     LAG(volumen_pedido_mes, 12) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC     ) as volumen_mismo_mes_ano_anterior,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📊 PROMEDIOS MÓVILES Y ACUMULADOS
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Promedio móvil 3 meses
# MAGIC     AVG(total_pedidos_mes) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC     ) as promedio_movil_3m_pedidos,
# MAGIC     
# MAGIC     AVG(volumen_pedido_mes) OVER (
# MAGIC       PARTITION BY cust_code 
# MAGIC       ORDER BY ano_pedido, mes_numero
# MAGIC       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC     ) as promedio_movil_3m_volumen,
# MAGIC     
# MAGIC     -- Acumulado del año
# MAGIC     SUM(total_pedidos_mes) OVER (
# MAGIC       PARTITION BY cust_code, ano_pedido
# MAGIC       ORDER BY mes_numero
# MAGIC       ROWS UNBOUNDED PRECEDING
# MAGIC     ) as pedidos_acumulados_ano,
# MAGIC     
# MAGIC     SUM(volumen_pedido_mes) OVER (
# MAGIC       PARTITION BY cust_code, ano_pedido
# MAGIC       ORDER BY mes_numero
# MAGIC       ROWS UNBOUNDED PRECEDING
# MAGIC     ) as volumen_acumulado_ano
# MAGIC     
# MAGIC   FROM demanda_con_metricas
# MAGIC ),
# MAGIC
# MAGIC -- ========================================
# MAGIC -- 🏆 RANKINGS Y PARTICIPACIONES
# MAGIC -- ========================================
# MAGIC demanda_con_rankings AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 🥇 RANKINGS GENERALES
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Ranking por pedidos
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ano_pedido, mes_numero
# MAGIC       ORDER BY total_pedidos_mes DESC
# MAGIC     ) as ranking_pedidos_general_mes,
# MAGIC     
# MAGIC     -- Ranking por volumen
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ano_pedido, mes_numero
# MAGIC       ORDER BY volumen_pedido_mes DESC
# MAGIC     ) as ranking_volumen_general_mes,
# MAGIC     
# MAGIC     -- Ranking por eficiencia
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ano_pedido, mes_numero
# MAGIC       ORDER BY eficiencia_entrega_mes_pct DESC
# MAGIC     ) as ranking_eficiencia_general_mes,
# MAGIC     
# MAGIC     -- Ranking por cumplimiento
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ano_pedido, mes_numero
# MAGIC       ORDER BY tasa_cumplimiento_completo_mes_pct DESC
# MAGIC     ) as ranking_cumplimiento_general_mes,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 📊 PARTICIPACIONES EN EL MERCADO
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Participación en pedidos totales
# MAGIC     ROUND(
# MAGIC       total_pedidos_mes * 100.0 / 
# MAGIC       SUM(total_pedidos_mes) OVER (PARTITION BY ano_pedido, mes_numero),
# MAGIC       3
# MAGIC     ) as participacion_pedidos_mercado_pct,
# MAGIC     
# MAGIC     -- Participación en volumen total
# MAGIC     ROUND(
# MAGIC       volumen_pedido_mes * 100.0 / 
# MAGIC       SUM(volumen_pedido_mes) OVER (PARTITION BY ano_pedido, mes_numero),
# MAGIC       3
# MAGIC     ) as participacion_volumen_mercado_pct,
# MAGIC     
# MAGIC     -- ========================================
# MAGIC     -- 🗺️ RANKINGS POR ZONA PREDOMINANTE
# MAGIC     -- ========================================
# MAGIC     
# MAGIC     -- Ranking en zona predominante
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY zona_predominante_mes, ano_pedido, mes_numero
# MAGIC       ORDER BY volumen_pedido_mes DESC
# MAGIC     ) as ranking_volumen_zona_predominante_mes,
# MAGIC     
# MAGIC     -- Participación en zona predominante
# MAGIC     ROUND(
# MAGIC       volumen_pedido_mes * 100.0 / 
# MAGIC       SUM(volumen_pedido_mes) OVER (PARTITION BY zona_predominante_mes, ano_pedido, mes_numero),
# MAGIC       2
# MAGIC     ) as participacion_volumen_zona_predominante_pct
# MAGIC     
# MAGIC   FROM demanda_con_tendencias
# MAGIC )
# MAGIC
# MAGIC -- ========================================
# MAGIC -- 🎯 VISTA FINAL CON MÉTRICAS EJECUTIVAS
# MAGIC -- ========================================
# MAGIC SELECT 
# MAGIC   -- ========================================
# MAGIC   -- 📋 DIMENSIONES PRINCIPALES
# MAGIC   -- ========================================
# MAGIC   cust_code,
# MAGIC   no_cli,
# MAGIC   ano_pedido,
# MAGIC   mes_numero,
# MAGIC   periodo_mes,
# MAGIC   trimestre_numero,
# MAGIC   periodo_trimestre,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 📊 MÉTRICAS DE DEMANDA PRINCIPALES
# MAGIC   -- ========================================
# MAGIC   dias_activos_mes,
# MAGIC   total_pedidos_mes,
# MAGIC   total_ordenes_mes,
# MAGIC   total_obras_mes,
# MAGIC   total_productos_mes,
# MAGIC   total_proyectos_mes,
# MAGIC   
# MAGIC   volumen_pedido_mes,
# MAGIC   volumen_entregado_mes,
# MAGIC   ROUND(volumen_promedio_pedido_mes, 2) as volumen_promedio_pedido_mes,
# MAGIC   ROUND(volumen_promedio_entregado_mes, 2) as volumen_promedio_entregado_mes,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🎯 INDICADORES DE RENDIMIENTO (KPIs)
# MAGIC   -- ========================================
# MAGIC   pedidos_promedio_por_dia_activo,
# MAGIC   volumen_promedio_por_dia_activo,
# MAGIC   obras_promedio_por_dia_activo,
# MAGIC   frecuencia_actividad_mes_pct,
# MAGIC   
# MAGIC   ROUND(cumplimiento_promedio_mes, 2) as cumplimiento_promedio_mes,
# MAGIC   eficiencia_entrega_mes_pct,
# MAGIC   tasa_cumplimiento_completo_mes_pct,
# MAGIC   
# MAGIC   pedidos_cumplidos_mes,
# MAGIC   pedidos_casi_completos_mes,
# MAGIC   pedidos_con_faltante_mes,
# MAGIC   pedidos_no_entregados_mes,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 📈 ANÁLISIS DE TENDENCIAS
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Crecimiento mes a mes
# MAGIC   CASE 
# MAGIC     WHEN pedidos_mes_anterior > 0 
# MAGIC     THEN ROUND(((total_pedidos_mes - pedidos_mes_anterior) * 100.0 / pedidos_mes_anterior), 2)
# MAGIC     ELSE NULL
# MAGIC   END as crecimiento_pedidos_mes_pct,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN volumen_mes_anterior > 0 
# MAGIC     THEN ROUND(((volumen_pedido_mes - volumen_mes_anterior) * 100.0 / volumen_mes_anterior), 2)
# MAGIC     ELSE NULL
# MAGIC   END as crecimiento_volumen_mes_pct,
# MAGIC   
# MAGIC   -- Crecimiento año a año
# MAGIC   CASE 
# MAGIC     WHEN pedidos_mismo_mes_ano_anterior > 0 
# MAGIC     THEN ROUND(((total_pedidos_mes - pedidos_mismo_mes_ano_anterior) * 100.0 / pedidos_mismo_mes_ano_anterior), 2)
# MAGIC     ELSE NULL
# MAGIC   END as crecimiento_pedidos_ano_pct,
# MAGIC   
# MAGIC   CASE 
# MAGIC     WHEN volumen_mismo_mes_ano_anterior > 0 
# MAGIC     THEN ROUND(((volumen_pedido_mes - volumen_mismo_mes_ano_anterior) * 100.0 / volumen_mismo_mes_ano_anterior), 2)
# MAGIC     ELSE NULL
# MAGIC   END as crecimiento_volumen_ano_pct,
# MAGIC   
# MAGIC   -- Promedios móviles
# MAGIC   ROUND(promedio_movil_3m_pedidos, 2) as promedio_movil_3m_pedidos,
# MAGIC   ROUND(promedio_movil_3m_volumen, 2) as promedio_movil_3m_volumen,
# MAGIC   
# MAGIC   -- Acumulados del año
# MAGIC   pedidos_acumulados_ano,
# MAGIC   volumen_acumulado_ano,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🗺️ PERFIL GEOGRÁFICO DEL CLIENTE
# MAGIC   -- ========================================
# MAGIC   zona_predominante_mes,
# MAGIC   cuadrante_predominante_mes,
# MAGIC   anillo_predominante_mes,
# MAGIC   densidad_predominante_mes,
# MAGIC   perfil_zona_predominante_mes,
# MAGIC   perfil_distancia_predominante_mes,
# MAGIC   
# MAGIC   -- Diversidad geográfica
# MAGIC   zonas_diferentes_mes,
# MAGIC   cuadrantes_diferentes_mes,
# MAGIC   anillos_diferentes_mes,
# MAGIC   densidades_diferentes_mes,
# MAGIC   
# MAGIC   -- Patrones geográficos
# MAGIC   patron_concentracion_predominante,
# MAGIC   dispersion_predominante,
# MAGIC   
# MAGIC   -- Métricas de cobertura
# MAGIC   ROUND(area_promedio_cobertura_mes, 2) as area_promedio_cobertura_mes,
# MAGIC   ROUND(area_maxima_cobertura_mes, 2) as area_maxima_cobertura_mes,
# MAGIC   ROUND(distancia_promedio_maxima_mes, 2) as distancia_promedio_maxima_mes,
# MAGIC   ROUND(distancia_maxima_obras_mes, 2) as distancia_maxima_obras_mes,
# MAGIC   
# MAGIC   -- Centro de gravedad del cliente
# MAGIC   ROUND(latitud_centro_gravedad_mes, 6) as latitud_centro_gravedad_mes,
# MAGIC   ROUND(longitud_centro_gravedad_mes, 6) as longitud_centro_gravedad_mes,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- ⏱️ PERFIL OPERACIONAL
# MAGIC   -- ========================================
# MAGIC   ROUND(tiempo_obra_promedio_mes, 1) as tiempo_obra_promedio_mes,
# MAGIC   ROUND(tiempo_espera_promedio_mes, 1) as tiempo_espera_promedio_mes,
# MAGIC   ROUND(tiempo_vaciado_promedio_mes, 1) as tiempo_vaciado_promedio_mes,
# MAGIC   ROUND(ciclo_total_promedio_mes, 1) as ciclo_total_promedio_mes,
# MAGIC   ROUND(eficiencia_promedio_mes, 2) as eficiencia_promedio_mes,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🏆 RANKINGS Y POSICIONAMIENTO
# MAGIC   -- ========================================
# MAGIC   ranking_pedidos_general_mes,
# MAGIC   ranking_volumen_general_mes,
# MAGIC   ranking_eficiencia_general_mes,
# MAGIC   ranking_cumplimiento_general_mes,
# MAGIC   ranking_volumen_zona_predominante_mes,
# MAGIC   
# MAGIC   -- Participaciones de mercado
# MAGIC   participacion_pedidos_mercado_pct,
# MAGIC   participacion_volumen_mercado_pct,
# MAGIC   participacion_volumen_zona_predominante_pct,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 📊 PATRONES DE COMPORTAMIENTO
# MAGIC   -- ========================================
# MAGIC   intensidad_predominante_mes,
# MAGIC   categoria_volumen_predominante_mes,
# MAGIC   performance_predominante_mes,
# MAGIC   diversidad_productos_predominante,
# MAGIC   dia_semana_mas_activo,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🚨 INDICADORES DE RIESGO Y ALERTAS
# MAGIC   -- ========================================
# MAGIC   dias_con_alertas_mes,
# MAGIC   ROUND((dias_con_alertas_mes * 100.0) / GREATEST(dias_activos_mes, 1), 2) as porcentaje_dias_con_alertas,
# MAGIC   tipo_alerta_predominante,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🎯 CLASIFICACIÓN ESTRATÉGICA DEL CLIENTE
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- Segmentación por volumen y frecuencia
# MAGIC   CASE 
# MAGIC     WHEN ranking_volumen_general_mes <= 10 AND frecuencia_actividad_mes_pct >= 80 THEN 'CLIENTE_ESTRATEGICO_A+'
# MAGIC     WHEN ranking_volumen_general_mes <= 20 AND frecuencia_actividad_mes_pct >= 60 THEN 'CLIENTE_ESTRATEGICO_A'
# MAGIC     WHEN ranking_volumen_general_mes <= 50 AND frecuencia_actividad_mes_pct >= 40 THEN 'CLIENTE_IMPORTANTE_B+'
# MAGIC     WHEN ranking_volumen_general_mes <= 100 AND frecuencia_actividad_mes_pct >= 20 THEN 'CLIENTE_IMPORTANTE_B'
# MAGIC     WHEN frecuencia_actividad_mes_pct >= 30 THEN 'CLIENTE_REGULAR_C+'
# MAGIC     WHEN frecuencia_actividad_mes_pct >= 10 THEN 'CLIENTE_REGULAR_C'
# MAGIC     ELSE 'CLIENTE_OCASIONAL_D'
# MAGIC   END as segmentacion_estrategica,
# MAGIC   
# MAGIC   -- Clasificación por crecimiento
# MAGIC   CASE 
# MAGIC     WHEN crecimiento_volumen_ano_pct >= 50 THEN 'CRECIMIENTO_ACELERADO'
# MAGIC     WHEN crecimiento_volumen_ano_pct >= 20 THEN 'CRECIMIENTO_ALTO'
# MAGIC     WHEN crecimiento_volumen_ano_pct >= 5 THEN 'CRECIMIENTO_MODERADO'
# MAGIC     WHEN crecimiento_volumen_ano_pct >= -5 THEN 'CRECIMIENTO_ESTABLE'
# MAGIC     WHEN crecimiento_volumen_ano_pct >= -20 THEN 'DECRECIMIENTO_MODERADO'
# MAGIC     ELSE 'DECRECIMIENTO_ALTO'
# MAGIC   END as clasificacion_crecimiento,
# MAGIC   
# MAGIC   -- Salud del cliente
# MAGIC   CASE 
# MAGIC     WHEN eficiencia_entrega_mes_pct >= 95 AND tasa_cumplimiento_completo_mes_pct >= 90 
# MAGIC          AND porcentaje_dias_con_alertas <= 10 THEN 'SALUD_EXCELENTE'
# MAGIC     WHEN eficiencia_entrega_mes_pct >= 85 AND tasa_cumplimiento_completo_mes_pct >= 80 
# MAGIC          AND porcentaje_dias_con_alertas <= 20 THEN 'SALUD_BUENA'
# MAGIC     WHEN eficiencia_entrega_mes_pct >= 70 AND tasa_cumplimiento_completo_mes_pct >= 70 
# MAGIC          AND porcentaje_dias_con_alertas <= 40 THEN 'SALUD_REGULAR'
# MAGIC     WHEN eficiencia_entrega_mes_pct >= 50 AND tasa_cumplimiento_completo_mes_pct >= 50 THEN 'SALUD_DEFICIENTE'
# MAGIC     ELSE 'SALUD_CRITICA'
# MAGIC   END as salud_cliente,
# MAGIC   
# MAGIC   -- ========================================
# MAGIC   -- 🕒 INFORMACIÓN TEMPORAL
# MAGIC   -- ========================================
# MAGIC   primera_fecha_actividad_mes,
# MAGIC   ultima_fecha_actividad_mes,
# MAGIC   current_timestamp() as fecha_calculo
# MAGIC
# MAGIC FROM demanda_con_rankings
# MAGIC ORDER BY 
# MAGIC   ano_pedido DESC,
# MAGIC   mes_numero DESC,
# MAGIC   ranking_volumen_general_mes ASC;
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- 📊 VISTA COMPLEMENTARIA: RESUMEN TRIMESTRAL POR CLIENTE
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW ${GOLD_DEMANDA_CLIENTE_TRIMESTRAL}
# MAGIC AS
# MAGIC SELECT 
# MAGIC   cust_code,
# MAGIC   no_cli,
# MAGIC   ano_pedido,
# MAGIC   trimestre_numero,
# MAGIC   periodo_trimestre,
# MAGIC   
# MAGIC   -- Agregaciones trimestrales
# MAGIC   COUNT(DISTINCT periodo_mes) as meses_activos_trimestre,
# MAGIC   SUM(total_pedidos_mes) as total_pedidos_trimestre,
# MAGIC   SUM(volumen_pedido_mes) as volumen_pedido_trimestre,
# MAGIC   SUM(volumen_entregado_mes) as volumen_entregado_trimestre,
# MAGIC   
# MAGIC   AVG(eficiencia_entrega_mes_pct) as eficiencia_promedio_trimestre,
# MAGIC   AVG(tasa_cumplimiento_completo_mes_pct) as cumplimiento_promedio_trimestre,
# MAGIC   
# MAGIC   -- Zona predominante del trimestre
# MAGIC   MODE(zona_predominante_mes) as zona_predominante_trimestre,
# MAGIC   MODE(segmentacion_estrategica) as segmentacion_predominante_trimestre,
# MAGIC   MODE(salud_cliente) as salud_predominante_trimestre,
# MAGIC   
# MAGIC   current_timestamp() as fecha_calculo
# MAGIC
# MAGIC FROM ${GOLD_DEMANDA_PEDIDOS}
# MAGIC GROUP BY 
# MAGIC   cust_code, no_cli, ano_pedido, trimestre_numero, periodo_trimestre
# MAGIC ORDER BY 
# MAGIC   ano_pedido DESC, trimestre_numero DESC, volumen_pedido_trimestre DESC;