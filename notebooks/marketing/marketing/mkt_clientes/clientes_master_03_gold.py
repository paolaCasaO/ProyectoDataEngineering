# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views only needs to be created the first time

# COMMAND ----------


dbutils.widgets.text('SILVER_CLIENTES', 'g6_mkt_clientes.silver.mv_clientes')
dbutils.widgets.text('GOLD_PERFIL_CLIENTES', 'g6_mkt_clientes.gold.vw_perfil_clientes')
dbutils.widgets.text('GOLD_DEMANDA_PEDIDOS', 'g6_cmc_pedidos.gold.vw_demanda_pedidos')
dbutils.widgets.text('GOLD_PUNTUALIDAD_CLIENTES', 'g6_ops_despachos.gold.vw_puntualidad_cliente')



# COMMAND ----------

SILVER_CLIENTES = dbutils.widgets.get("SILVER_CLIENTES") 
GOLD_PERFIL_CLIENTES = dbutils.widgets.get("GOLD_PERFIL_CLIENTES")
GOLD_DEMANDA_PEDIDOS = dbutils.widgets.get("GOLD_DEMANDA_PEDIDOS")
GOLD_PUNTUALIDAD_CLIENTES = dbutils.widgets.get("GOLD_PUNTUALIDAD_CLIENTES")


# COMMAND ----------

print('SILVER_CLIENTES\t:', SILVER_CLIENTES)
print('GOLD_PERFIL_CLIENTES\t:', GOLD_PERFIL_CLIENTES)
print('GOLD_DEMANDA_PEDIDOS\t:', GOLD_DEMANDA_PEDIDOS)
print('GOLD_PUNTUALIDAD_CLIENTES\t:', GOLD_PUNTUALIDAD_CLIENTES)


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${GOLD_PERFIL_CLIENTES} 
# MAGIC AS
# MAGIC SELECT 
# MAGIC   -- ========================================
# MAGIC   -- 📋 CAMPOS ESENCIALES (MAX 10)
# MAGIC   -- ========================================
# MAGIC   
# MAGIC   -- 1. Identificador único del cliente
# MAGIC   c.cust_code,
# MAGIC   
# MAGIC   -- 2. Nombre/marca del cliente
# MAGIC   c.brand_name,
# MAGIC   
# MAGIC   -- 3. Segmentación corporativa
# MAGIC   c.corporative_segmentation,
# MAGIC   
# MAGIC   -- 4. Volumen de demanda último mes
# MAGIC   COALESCE(d.volumen_pedido_mes, 0) as volumen_pedido_mes,
# MAGIC   
# MAGIC   -- 5. Puntualidad en entregas (%)
# MAGIC   COALESCE(p.tasa_puntualidad_pct, 0) as tasa_puntualidad_pct,
# MAGIC   
# MAGIC   -- 6. Zona geográfica predominante
# MAGIC   COALESCE(d.zona_predominante_mes, 'Sin Zona') as zona_principal,
# MAGIC   
# MAGIC   -- 7. Score operacional combinado
# MAGIC   ROUND(
# MAGIC     COALESCE(
# MAGIC       (COALESCE(p.tasa_puntualidad_pct, 0) * 0.5) +           -- 50% puntualidad
# MAGIC       (COALESCE(d.eficiencia_entrega_mes_pct, 0) * 0.3) + -- 30% eficiencia
# MAGIC       (COALESCE(d.tasa_cumplimiento_completo_mes_pct, 0) * 0.2), -- 20% cumplimiento
# MAGIC       0
# MAGIC     ), 1
# MAGIC   ) as score_operacional,
# MAGIC   
# MAGIC   -- 8. Clasificación final del cliente
# MAGIC   CASE 
# MAGIC     WHEN c.corporative_segmentation = 'PREMIUM' 
# MAGIC          AND COALESCE(p.tasa_puntualidad_pct, 0) >= 90 
# MAGIC          AND COALESCE(d.volumen_pedido_mes, 0) >= 1000 THEN 'VIP'
# MAGIC     WHEN c.corporative_segmentation = 'PREMIUM' 
# MAGIC          AND COALESCE(p.tasa_puntualidad_pct, 0) >= 75 THEN 'PREMIUM'
# MAGIC     WHEN c.corporative_segmentation = 'CORPORATIVO' 
# MAGIC          AND COALESCE(p.tasa_puntualidad_pct, 0) >= 80 THEN 'CORPORATIVO_A'
# MAGIC     WHEN c.corporative_segmentation = 'CORPORATIVO' THEN 'CORPORATIVO_B'
# MAGIC     WHEN COALESCE(d.frecuencia_actividad_mes_pct, 0) >= 80 
# MAGIC          AND COALESCE(d.volumen_pedido_mes, 0) >= 300 THEN 'FRECUENTE'
# MAGIC     WHEN COALESCE(p.tasa_puntualidad_pct, 0) < 50 
# MAGIC          OR COALESCE(d.porcentaje_dias_con_alertas, 0) > 50 THEN 'RIESGO'
# MAGIC     WHEN COALESCE(d.volumen_pedido_mes, 0) < 50 THEN 'OCASIONAL'
# MAGIC     ELSE 'REGULAR'
# MAGIC   END as clasificacion_cliente,
# MAGIC   
# MAGIC   -- 9. 
# MAGIC   COALESCE(d.ano_pedido, 'Sin Actividad') as ultimo_periodo_activo,
# MAGIC   
# MAGIC   -- 10. Estado del cliente
# MAGIC   CASE 
# MAGIC     WHEN c.active_flg = 'Y' AND COALESCE(d.volumen_pedido_mes, 0) > 0 THEN 'ACTIVO'
# MAGIC     WHEN c.active_flg = 'Y' AND COALESCE(d.volumen_pedido_mes, 0) = 0 THEN 'INACTIVO'
# MAGIC     ELSE 'SUSPENDIDO'
# MAGIC   END as estado_cliente
# MAGIC
# MAGIC FROM ${SILVER_CLIENTES} c
# MAGIC
# MAGIC -- LEFT JOIN con vista de demanda (datos más recientes por cliente)
# MAGIC LEFT JOIN (
# MAGIC   SELECT 
# MAGIC     cust_code,
# MAGIC     volumen_pedido_mes,
# MAGIC     eficiencia_entrega_mes_pct,
# MAGIC     tasa_cumplimiento_completo_mes_pct,
# MAGIC     frecuencia_actividad_mes_pct,
# MAGIC     porcentaje_dias_con_alertas,
# MAGIC     zona_predominante_mes,
# MAGIC     ano_pedido,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY cust_code ORDER BY ano_pedido DESC, mes_numero DESC) as rn
# MAGIC   FROM ${GOLD_DEMANDA_PEDIDOS} 
# MAGIC ) d ON c.cust_code = d.cust_code AND d.rn = 1
# MAGIC
# MAGIC -- LEFT JOIN con vista de puntualidad
# MAGIC LEFT JOIN ${GOLD_PUNTUALIDAD_CLIENTES}  p 
# MAGIC   ON c.cust_code = p.cust_code
# MAGIC
# MAGIC ORDER BY 
# MAGIC   -- Ordenar por importancia: VIP primero, luego por score operacional
# MAGIC   CASE clasificacion_cliente
# MAGIC     WHEN 'VIP' THEN 1
# MAGIC     WHEN 'PREMIUM' THEN 2
# MAGIC     WHEN 'CORPORATIVO_A' THEN 3
# MAGIC     WHEN 'FRECUENTE' THEN 4
# MAGIC     WHEN 'CORPORATIVO_B' THEN 5
# MAGIC     WHEN 'REGULAR' THEN 6
# MAGIC     WHEN 'OCASIONAL' THEN 7
# MAGIC     WHEN 'RIESGO' THEN 8
# MAGIC     ELSE 9
# MAGIC   END,
# MAGIC   score_operacional DESC,
# MAGIC   volumen_pedido_mes DESC;
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g6_mkt_clientes.gold.vw_perfil_clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g6_cmc_pedidos.gold.vw_demanda_pedidos