# Databricks notebook source
BRONZE_TB_RAW_DESPACHOS = 'g6_ops_despachos.bronze.raw_despachos'
SILVER_MV_DESPACHOS_CICLO = 'g6_ops_despachos.silver.mv_despachos_ciclo'
SILVER_MV_DESPACHOS_OTIF = 'g6_ops_despachos.silver.mv_despachos_otif'
SILVER_MV_DESPACHOS_PTA = 'g6_ops_despachos.silver.mv_despachos_pta'


# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {BRONZE_TB_RAW_DESPACHOS}')

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {SILVER_MV_DESPACHOS_PTA}')

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {SILVER_MV_DESPACHOS_CICLO}')

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {SILVER_MV_DESPACHOS_OTIF}')

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {BRONZE_TB_RAW_DESPACHOS} (
      proj_code STRING COMMENT 'Código del proyecto',
      order_date TIMESTAMP COMMENT 'Fecha y hora de la orden',
      order_code STRING COMMENT 'Código de la orden',
      order_code2 STRING COMMENT 'Código alternativo de la orden',
      cust_code STRING COMMENT 'Código del cliente',
      no_cli STRING COMMENT 'Número de cliente',
      co_obr STRING COMMENT 'Código de obra',
      de_obr STRING COMMENT 'Descripción de la obra',
      obra_latitud DOUBLE COMMENT 'Latitud de la ubicación de la obra',
      obra_longitud DOUBLE COMMENT 'Longitud de la ubicación de la obra',
      tkt_code STRING COMMENT 'Código del ticket',
      guia STRING COMMENT 'Número de guía',
      pivot_plant_code STRING COMMENT 'Código de la planta pivot',
      pivot_plant_name STRING COMMENT 'Nombre de la planta pivot',
      pivot_plant_latitud DOUBLE COMMENT 'Latitud de la planta pivot',
      pivot_plant_longitud DOUBLE COMMENT 'Longitud de la planta pivot',
      plant_code STRING COMMENT 'Código de la planta',
      plant_name STRING COMMENT 'Nombre de la planta',
      plant_latitud DOUBLE COMMENT 'Latitud de la planta',
      plant_longitud DOUBLE COMMENT 'Longitud de la planta',
      truck_code STRING COMMENT 'Código del camión',
      short_prod_descr STRING COMMENT 'Descripción corta del producto',
      delv_qty DOUBLE COMMENT 'Cantidad entregada',
      descr STRING COMMENT 'Descripción del producto',
      to_job_time TIMESTAMP COMMENT 'Hora de salida hacia el trabajo',
      on_job_time TIMESTAMP COMMENT 'Hora de llegada al trabajo',
      begin_unld_time TIMESTAMP COMMENT 'Hora de inicio de descarga',
      end_unld_time TIMESTAMP COMMENT 'Hora de fin de descarga',
      to_plant_time TIMESTAMP COMMENT 'Hora de salida hacia la planta',
      at_plant_time TIMESTAMP COMMENT 'Hora de llegada a la planta',
      tiempo_a_obra DOUBLE COMMENT 'Tiempo en minutos desde planta hasta obra',
      tiempo_espera DOUBLE COMMENT 'Tiempo de espera en minutos en la obra',
      tiempo_vaciado DOUBLE COMMENT 'Tiempo de vaciado en minutos',
      tiempo_salida DOUBLE COMMENT 'Tiempo de salida en minutos desde la obra',
      tiempo_a_planta DOUBLE COMMENT 'Tiempo en minutos de regreso a la planta',
      order_qty DOUBLE COMMENT 'Cantidad ordenada',
      start_time TIMESTAMP COMMENT 'Hora de inicio del despacho',
      frecuencia INT COMMENT 'Frecuencia del despacho',
      prod_code STRING COMMENT 'Código del producto',
      truck_type STRING COMMENT 'Tipo de camión',
      load_num STRING COMMENT 'Número de carga',
      tamano_de_carga STRING COMMENT 'Tamaño de la carga',
      sched_num STRING COMMENT 'Número de programación',
      file_date DATE COMMENT 'Fecha del archivo procesado (YYYYMMDD)',
      ingestion_timestamp TIMESTAMP COMMENT 'Timestamp de cuando se ingirió el registro',
      *rescued*data STRING COMMENT 'Datos adicionales o no estructurados rescatados durante la ingesta'
    )
    USING DELTA
    COMMENT 'Tabla raw de despachos de concreto con información de órdenes, plantas, camiones y tiempos de entrega'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """
)

print(f"Tabla {BRONZE_TB_RAW_DESPACHOS} creada exitosamente")

# COMMAND ----------

def create_otif_materialized_view():
    
    # Crear la vista materializada
    spark.sql(f"""
    CREATE MATERIALIZED VIEW {SILVER_MV_DESPACHOS_OTIF}
    REFRESH EVERY 1 DAY
    COMMENT 'Vista materializada de análisis OTIF (On Time In Full) por orden de despacho'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    AS
    SELECT 
      order_code,
      CAST(order_date AS DATE) as order_date,
      cust_code,
      de_obr,
      plant_name,
      SUM(order_qty) as order_qty,
      SUM(delv_qty) as delv_qty,
      COUNT(*) as total_despachos,
      SUM(CASE WHEN delv_qty >= order_qty THEN 1 ELSE 0 END) as despachos_completos,
      MIN(start_time) as planned_delivery_time,
      MAX(on_job_time) as actual_delivery_time,
      (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 as delivery_delay_minutes,
      ROUND((SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) * 100, 2) as qty_fill_rate,
      CASE WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 THEN true ELSE false END as is_on_time,
      CASE WHEN (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN true ELSE false END as is_in_full,
      CASE 
        WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
             AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN true 
        ELSE false 
      END as is_otif,
      CASE 
        WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
             AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN 'Perfect'
        WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 > 30 
             AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) >= 0.95 THEN 'Late'
        WHEN (UNIX_TIMESTAMP(MAX(on_job_time)) - UNIX_TIMESTAMP(MIN(start_time))) / 60.0 <= 30 
             AND (SUM(delv_qty) / NULLIF(SUM(order_qty), 0)) < 0.95 THEN 'Short'
        ELSE 'Late&Short'
      END as otif_category,
      file_date,
      current_timestamp() as created_at
    FROM {BRONZE_TB_RAW_DESPACHOS}
    WHERE order_code IS NOT NULL
      AND order_date IS NOT NULL
    GROUP BY order_code, CAST(order_date AS DATE), cust_code, de_obr, plant_name, file_date
    """)
    
    # Diccionario de comentarios
    column_comments = {
        'order_code': 'Código de la orden',
        'order_date': 'Fecha de la orden',
        'cust_code': 'Código del cliente',
        'de_obr': 'Descripción de la obra',
        'plant_name': 'Nombre de la planta',
        'order_qty': 'Cantidad ordenada',
        'delv_qty': 'Cantidad entregada',
        'total_despachos': 'Total de despachos para esta orden',
        'despachos_completos': 'Despachos con cantidad completa',
        'planned_delivery_time': 'Hora planificada de entrega',
        'actual_delivery_time': 'Hora real de entrega (on_job_time)',
        'delivery_delay_minutes': 'Retraso en minutos (+ = tarde, - = temprano)',
        'qty_fill_rate': 'Tasa de cumplimiento de cantidad (%)',
        'is_on_time': 'Entregado a tiempo (<=30 min de tolerancia)',
        'is_in_full': 'Entregado cantidad completa (>=95%)',
        'is_otif': 'On Time In Full (ambos criterios cumplidos)',
        'otif_category': 'Categoría OTIF: Perfect, Late, Short, Late&Short',
        'file_date': 'Fecha del archivo fuente',
        'created_at': 'Timestamp de creación del registro'
    }
    
    # Agregar comentarios
    for column, comment in column_comments.items():
        try:
            spark.sql(f"""
            ALTER TABLE {SILVER_MV_DESPACHOS_OTIF} 
            ALTER COLUMN {column} COMMENT '{comment}'
            """)
        except Exception as e:
            print(f"⚠️ No se pudo agregar comentario para {column}: {e}")
    
    print(f"✅ Vista materializada {SILVER_MV_DESPACHOS_OTIF} creada exitosamente")

# Ejecutar
create_otif_materialized_view()

# COMMAND ----------

def create_despachos_planta_materialized_view():
    
    # Crear la vista materializada
    spark.sql(f"""
    CREATE MATERIALIZED VIEW {SILVER_MV_DESPACHOS_PLANTA}
    REFRESH EVERY 1 HOUR
    COMMENT 'Vista materializada de análisis agregado de despachos por planta'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    AS
    SELECT 
      plant_code,
      plant_name,
      plant_latitud,
      plant_longitud,
      CAST(order_date AS DATE) as fecha_analisis,
      COUNT(*) as total_despachos,
      COUNT(DISTINCT order_code) as total_ordenes,
      COUNT(DISTINCT cust_code) as total_clientes,
      SUM(delv_qty) as volumen_total,
      AVG(delv_qty) as volumen_promedio,
      AVG(tiempo_a_obra + tiempo_espera + tiempo_vaciado + tiempo_a_planta) as tiempo_ciclo_promedio,
      AVG(tiempo_a_obra) as tiempo_a_obra_promedio,
      AVG(tiempo_espera) as tiempo_espera_promedio,
      AVG(tiempo_vaciado) as tiempo_vaciado_promedio,
      AVG(tiempo_a_planta) as tiempo_regreso_promedio,
      AVG((tiempo_a_obra + tiempo_espera + tiempo_vaciado + tiempo_a_planta) / 480.0 * 100) as utilizacion_camiones,
      SUM(CASE WHEN (delv_qty >= order_qty * 0.95) AND 
           (UNIX_TIMESTAMP(on_job_time) - UNIX_TIMESTAMP(start_time)) / 60.0 <= 30 THEN 1 ELSE 0 END) as despachos_otif,
      AVG(CASE WHEN (delv_qty >= order_qty * 0.95) AND 
           (UNIX_TIMESTAMP(on_job_time) - UNIX_TIMESTAMP(start_time)) / 60.0 <= 30 THEN 100.0 ELSE 0.0 END) as tasa_otif,
      COUNT(DISTINCT prod_code) as productos_despachados,
      file_date,
      current_timestamp() as created_at
    FROM {BRONZE_TB_RAW_DESPACHOS}
    WHERE plant_code IS NOT NULL
    GROUP BY plant_code, plant_name, plant_latitud, plant_longitud, CAST(order_date AS DATE), file_date
    """)
    
    # Diccionario de comentarios para las columnas
    column_comments = {
        'plant_code': 'Código de la planta',
        'plant_name': 'Nombre de la planta',
        'plant_latitud': 'Latitud de la planta',
        'plant_longitud': 'Longitud de la planta',
        'fecha_analisis': 'Fecha del análisis',
        'total_despachos': 'Total de despachos',
        'total_ordenes': 'Total de órdenes únicas',
        'total_clientes': 'Total de clientes únicos',
        'volumen_total': 'Volumen total despachado',
        'volumen_promedio': 'Volumen promedio por despacho',
        'tiempo_ciclo_promedio': 'Tiempo de ciclo promedio (minutos)',
        'tiempo_a_obra_promedio': 'Tiempo promedio a obra (minutos)',
        'tiempo_espera_promedio': 'Tiempo promedio de espera (minutos)',
        'tiempo_vaciado_promedio': 'Tiempo promedio de vaciado (minutos)',
        'tiempo_regreso_promedio': 'Tiempo promedio de regreso (minutos)',
        'utilizacion_camiones': 'Utilización promedio de camiones (%)',
        'despachos_otif': 'Despachos On Time In Full',
        'tasa_otif': 'Tasa OTIF (%)',
        'productos_despachados': 'Cantidad de productos diferentes',
        'file_date': 'Fecha del archivo fuente',
        'created_at': 'Timestamp de creación del registro'
    }
    
    # Agregar comentarios a las columnas
    for column, comment in column_comments.items():
        try:
            spark.sql(f"""
            ALTER TABLE {SILVER_MV_DESPACHOS_PLANTA} 
            ALTER COLUMN {column} COMMENT '{comment}'
            """)
        except Exception as e:
            print(f"⚠️ No se pudo agregar comentario para {column}: {e}")
    
    print(f"✅ Vista materializada {SILVER_MV_DESPACHOS_PLANTA} creada exitosamente")

# Ejecutar
create_despachos_planta_materialized_view()

# COMMAND ----------

def create_despachos_ciclo_materialized_view():
    
    # Crear la vista materializada
    spark.sql(f"""
    CREATE MATERIALIZED VIEW {SILVER_MV_DESPACHOS_CICLO}
    REFRESH EVERY 1 HOUR
    COMMENT 'Vista materializada de análisis detallado del tiempo del ciclo de despachos'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    AS
    SELECT 
      CONCAT(order_code, '_', truck_code, '_', DATE_FORMAT(start_time, 'yyyyMMddHHmm')) as despacho_id,
      order_code,
      truck_code,
      plant_name,
      CAST(order_date AS DATE) as fecha_despacho,
      start_time,
      on_job_time,
      at_plant_time,
      COALESCE(tiempo_a_obra, 0) as tiempo_a_obra,
      COALESCE(tiempo_espera, 0) as tiempo_espera,
      COALESCE(tiempo_vaciado, 0) as tiempo_vaciado,
      COALESCE(tiempo_salida, 0) as tiempo_salida,
      COALESCE(tiempo_a_planta, 0) as tiempo_a_planta,
      (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) as tiempo_ciclo_total,
      COALESCE(tiempo_vaciado, 0) as tiempo_productivo,
      (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) as tiempo_transporte,
      (COALESCE(tiempo_espera, 0) + COALESCE(tiempo_salida, 0)) as tiempo_no_productivo,
      CASE 
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
        THEN ROUND((COALESCE(tiempo_vaciado, 0) / (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0))) * 100, 2)
        ELSE 0 
      END as eficiencia_ciclo,
      CASE 
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 120 THEN 'Rápido'
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 180 THEN 'Normal'
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) <= 240 THEN 'Lento'
        ELSE 'Muy Lento'
      END as categoria_ciclo,
      ((COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) / 60.0) * 30 as distancia_estimada,
      CASE 
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
        THEN 30.0
        ELSE 0 
      END as velocidad_promedio,
      COALESCE(delv_qty, 0) as delv_qty,
      CASE 
        WHEN (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0)) > 0 
        THEN COALESCE(delv_qty, 0) / (COALESCE(tiempo_a_obra, 0) + COALESCE(tiempo_espera, 0) + COALESCE(tiempo_vaciado, 0) + COALESCE(tiempo_salida, 0) + COALESCE(tiempo_a_planta, 0))
        ELSE 0 
      END as productividad,
      file_date,
      current_timestamp() as created_at
    FROM {BRONZE_TB_RAW_DESPACHOS}
    WHERE start_time IS NOT NULL 
      AND order_code IS NOT NULL
      AND truck_code IS NOT NULL
      AND plant_name IS NOT NULL
    """)
    
    # Diccionario de comentarios para las columnas
    column_comments = {
        'despacho_id': 'Identificador único del despacho',
        'order_code': 'Código de la orden',
        'truck_code': 'Código del camión',
        'plant_name': 'Nombre de la planta',
        'fecha_despacho': 'Fecha del despacho',
        'start_time': 'Hora de inicio',
        'on_job_time': 'Hora de llegada a obra',
        'at_plant_time': 'Hora de regreso a planta',
        'tiempo_a_obra': 'Tiempo a obra (minutos)',
        'tiempo_espera': 'Tiempo de espera (minutos)',
        'tiempo_vaciado': 'Tiempo de vaciado (minutos)',
        'tiempo_salida': 'Tiempo de salida (minutos)',
        'tiempo_a_planta': 'Tiempo de regreso (minutos)',
        'tiempo_ciclo_total': 'Tiempo total del ciclo (minutos)',
        'tiempo_productivo': 'Tiempo productivo: vaciado (minutos)',
        'tiempo_transporte': 'Tiempo de transporte total (minutos)',
        'tiempo_no_productivo': 'Tiempo de espera total (minutos)',
        'eficiencia_ciclo': 'Eficiencia del ciclo (%): productivo/total',
        'categoria_ciclo': 'Categoría: Rápido, Normal, Lento, Muy Lento',
        'distancia_estimada': 'Distancia estimada basada en tiempo de viaje',
        'velocidad_promedio': 'Velocidad promedio estimada (km/h)',
        'delv_qty': 'Cantidad entregada',
        'productividad': 'Productividad: cantidad/tiempo_ciclo',
        'file_date': 'Fecha del archivo fuente',
        'created_at': 'Timestamp de creación del registro'
    }
    
    # Agregar comentarios a las columnas
    for column, comment in column_comments.items():
        try:
            spark.sql(f"""
            ALTER TABLE {SILVER_MV_DESPACHOS_CICLO} 
            ALTER COLUMN {column} COMMENT '{comment}'
            """)
        except Exception as e:
            print(f"⚠️ No se pudo agregar comentario para {column}: {e}")
    
    print(f"✅ Vista materializada {SILVER_MV_DESPACHOS_CICLO} creada exitosamente")

# Ejecutar
create_despachos_ciclo_materialized_view()