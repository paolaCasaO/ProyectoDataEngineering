# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline
# MAGIC - Views and Materialized Views only needs to be created the first time
# MAGIC - Materialized views only will beed to be REFRESHED (Point to DataPlatform's REFRESH notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE SERVERLESS COMPUTE

# COMMAND ----------

dbutils.widgets.text('BRONZE_TB_RAW_CLIENTES', 'g6_mkt_clientes.bronze.raw_clientes')
dbutils.widgets.text('SILVER_CLIENTES', 'g6_mkt_clientes.silver.mv_clientes')

# COMMAND ----------

BRONZE_TB_RAW_CLIENTES = dbutils.widgets.get("BRONZE_TB_RAW_CLIENTES")
SILVER_CLIENTES = dbutils.widgets.get("SILVER_CLIENTES")
print('SILVER_CLIENTES\t\t:', dbutils.widgets.get("SILVER_CLIENTES")) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE WAREHOUSE COMPUTE
# MAGIC Materialized Views only run in Warehouse Compute

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP MATERIALIZED VIEW IF EXISTS ${SILVER_CLIENTES};
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW ${SILVER_CLIENTES} 
# MAGIC AS
# MAGIC SELECT  
# MAGIC   cust_code,
# MAGIC   no_cli,
# MAGIC   brand_name,
# MAGIC   active_flg,
# MAGIC   -- Segmentación corporativa
# MAGIC   corporative_segmentation,
# MAGIC   client_type,
# MAGIC   client_nature,
# MAGIC   client_origin,
# MAGIC   -- Comportamiento del cliente
# MAGIC   customer_behaviour_calification,
# MAGIC   customer_behaviour_ranking,
# MAGIC   average_delay,
# MAGIC   -- Información financiera para segmentación
# MAGIC   available_credit,
# MAGIC   credit_line,
# MAGIC   ROUND((available_credit / NULLIF(credit_line, 0)) * 100, 2) as credit_utilization_pct,
# MAGIC   
# MAGIC   -- Segmentación por comportamiento de pago
# MAGIC   CASE 
# MAGIC     WHEN average_delay <= 5 THEN 'Excelente'
# MAGIC     WHEN average_delay <= 15 THEN 'Bueno'
# MAGIC     WHEN average_delay <= 30 THEN 'Regular'
# MAGIC     ELSE 'Deficiente'
# MAGIC   END as segmento_pago,
# MAGIC   
# MAGIC   -- Segmentación por crédito
# MAGIC   CASE 
# MAGIC     WHEN credit_line >= 500000 THEN 'Alto'
# MAGIC     WHEN credit_line >= 100000 THEN 'Medio'
# MAGIC     WHEN credit_line >= 20000 THEN 'Bajo'
# MAGIC     ELSE 'Mínimo'
# MAGIC   END as segmento_credito,
# MAGIC   
# MAGIC   -- Segmentación combinada
# MAGIC   CASE 
# MAGIC     WHEN corporative_segmentation = 'PREMIUM' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' 
# MAGIC          AND average_delay <= 10 THEN 'VIP'
# MAGIC     WHEN corporative_segmentation = 'PREMIUM' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 'Premium'
# MAGIC     WHEN corporative_segmentation = 'CORPORATIVO' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' 
# MAGIC          AND average_delay <= 15 THEN 'Corporativo A'
# MAGIC     WHEN corporative_segmentation = 'CORPORATIVO' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 'Corporativo B'
# MAGIC     WHEN corporative_segmentation = 'MASIVO' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 'Masivo A'
# MAGIC     WHEN customer_behaviour_calification = 'MALO' THEN 'Riesgo'
# MAGIC     ELSE 'Estándar'
# MAGIC   END as segmento_principal,
# MAGIC   
# MAGIC   -- Potencial de crecimiento
# MAGIC   CASE 
# MAGIC     WHEN credit_utilization_pct <= 30 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 'Alto Potencial'
# MAGIC     WHEN credit_utilization_pct <= 60 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 'Medio Potencial'
# MAGIC     WHEN credit_utilization_pct > 90 THEN 'Saturado'
# MAGIC     ELSE 'Bajo Potencial'
# MAGIC   END as potencial_crecimiento,
# MAGIC   
# MAGIC   -- Prioridad comercial
# MAGIC   CASE 
# MAGIC     WHEN corporative_segmentation = 'PREMIUM' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 1
# MAGIC     WHEN corporative_segmentation = 'CORPORATIVO' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 2
# MAGIC     WHEN corporative_segmentation = 'MASIVO' 
# MAGIC          AND customer_behaviour_calification = 'BUENO' THEN 3
# MAGIC     WHEN customer_behaviour_calification = 'MALO' THEN 5
# MAGIC     ELSE 4
# MAGIC   END as prioridad_comercial,
# MAGIC   
# MAGIC   -- Información geográfica para segmentación
# MAGIC   country_name,
# MAGIC   region_name,
# MAGIC   zone_name,
# MAGIC
# MAGIC   current_timestamp() as created_at
# MAGIC   
# MAGIC FROM ${BRONZE_TB_RAW_CLIENTES};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Análisis de clientes por segmento principal
# MAGIC SELECT 
# MAGIC   segmento_principal,
# MAGIC   COUNT(*) as total_clientes,
# MAGIC   ROUND(AVG(credit_utilization_pct), 2) as utilizacion_credito_promedio,
# MAGIC   ROUND(AVG(average_delay), 1) as delay_promedio_dias,
# MAGIC   ROUND(AVG(credit_line), 0) as linea_credito_promedio,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje_total
# MAGIC FROM ${SILVER_CLIENTES}
# MAGIC GROUP BY segmento_principal
# MAGIC ORDER BY total_clientes DESC;