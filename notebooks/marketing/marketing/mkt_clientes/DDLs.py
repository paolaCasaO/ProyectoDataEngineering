# Databricks notebook source
BRONZE_TB_RAW_CLIENTES = 'g6_mkt_clientes.bronze.raw_clientes'
SILVER_TB_CLIENTES = 'g6_mkt_clientes.silver.mv_clientes'


# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {BRONZE_TB_RAW_CLIENTES}')

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {SILVER_TB_CLIENTES}')

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {BRONZE_TB_RAW_CLIENTES} (
      cust_code STRING COMMENT 'Código único del cliente',
      no_cli STRING COMMENT 'Número de cliente',
      active_flg BOOLEAN COMMENT 'Flag indicador de cliente activo (TRUE/FALSE)',
      available_credit DOUBLE COMMENT 'Crédito disponible del cliente',
      average_delay DOUBLE COMMENT 'Promedio de días de retraso en pagos',
      brand_name STRING COMMENT 'Marca o nombre comercial del cliente',
      client_address STRING COMMENT 'Dirección física del cliente',
      client_currency_code STRING COMMENT 'Código de moneda del cliente (PEN = Soles Peruanos)',
      client_desc STRING COMMENT 'Descripción detallada del cliente',
      client_dimkey STRING COMMENT 'Clave dimensional del cliente para el data warehouse',
      client_fax STRING COMMENT 'Número de fax del cliente',
      client_nature STRING COMMENT 'Naturaleza del cliente (NATURAL = Persona Natural)',
      client_origin STRING COMMENT 'Origen del cliente (MARKETING, VENTA_DIRECTA)',
      client_parent_holding_id STRING COMMENT 'ID del holding padre o empresa matriz',
      client_parent_holding_name STRING COMMENT 'Nombre del holding padre o empresa matriz',
      client_phone STRING COMMENT 'Número de teléfono del cliente',
      client_source_name STRING COMMENT 'Sistema fuente de origen (SAP, ORACLE)',
      client_type STRING COMMENT 'Tipo de cliente (CONSTRUCTOR, GOBIERNO, PARTICULAR)',
      co_cli_concremax STRING COMMENT 'Código de clasificación CONCREMAX',
      co_cli_unicon STRING COMMENT 'Código de clasificación UNICON',
      corporative_segmentation STRING COMMENT 'Segmentación corporativa (PREMIUM, CORPORATIVO, MASIVO)',
      country_name STRING COMMENT 'País donde se encuentra el cliente',
      created_ts TIMESTAMP COMMENT 'Timestamp de creación del registro del cliente',
      credit_line DOUBLE COMMENT 'Línea de crédito asignada al cliente',
      customer_behaviour_calification STRING COMMENT 'Calificación de comportamiento del cliente (BUENO, MALO)',
      customer_behaviour_ranking INT COMMENT 'Ranking numérico de comportamiento del cliente (1-7)',
      district_name STRING COMMENT 'Distrito donde se encuentra el cliente',
      dwh_created_ts TIMESTAMP COMMENT 'Timestamp de creación del registro en el data warehouse',
      dwh_modified_ts TIMESTAMP COMMENT 'Timestamp de última modificación en el data warehouse',
      effective_end_ts TIMESTAMP COMMENT 'Fecha y hora de fin de vigencia del registro',
      effective_start_ts TIMESTAMP COMMENT 'Fecha y hora de inicio de vigencia del registro',
      email_id STRING COMMENT 'Dirección de correo electrónico del cliente',
      last_modified_ts TIMESTAMP COMMENT 'Timestamp de última modificación del registro',
      person_department_name STRING COMMENT 'Departamento de la persona de contacto',
      province_name STRING COMMENT 'Provincia donde se encuentra el cliente',
      quadrant_name STRING COMMENT 'Cuadrante geográfico del cliente',
      region_name STRING COMMENT 'Región donde se encuentra el cliente',
      salesforce_id STRING COMMENT 'ID del cliente en el sistema Salesforce',
      urbanization_name STRING COMMENT 'Urbanización donde se encuentra el cliente',
      zip_code STRING COMMENT 'Código postal del cliente',
      zone_name STRING COMMENT 'Zona geográfica del cliente (ZONA_SUR, ZONA_ESTE)',
      file_date DATE COMMENT 'Fecha del archivo procesado (YYYYMMDD)',
      ingestion_timestamp TIMESTAMP COMMENT 'Timestamp de cuando se ingirió el registro',
      rescued_data STRING COMMENT 'Datos adicionales o no estructurados rescatados durante la ingesta'
    )
    USING DELTA
    COMMENT 'Tabla raw de información de clientes con datos demográficos, financieros, geográficos y de comportamiento'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    """
)
print(f"Tabla {BRONZE_TB_RAW_CLIENTES} creada exitosamente")

# COMMAND ----------

def create_clientes_materialized_view():
    
    # Crear la vista materializada
    spark.sql(f"""
    CREATE MATERIALIZED VIEW {SILVER_CLIENTES}
    REFRESH EVERY 1 DAY
    COMMENT 'Vista materializada de clientes con antigüedad, crédito e información geográfica'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    )
    AS
    SELECT 
      cust_code,
      no_cli,
      brand_name,
      client_desc,
      active_flg,
      
      -- Información de crédito
      available_credit,
      credit_line,
      ROUND((available_credit / NULLIF(credit_line, 0)) * 100, 2) as credit_utilization_pct,
      
      -- Antigüedad del cliente
      created_ts,
      DATEDIFF(CURRENT_DATE(), CAST(created_ts AS DATE)) as days_since_creation,
      
      -- Información geográfica
      country_name,
      region_name,
      province_name,
      district_name,
      zone_name,
      client_address,
      zip_code,
      
      -- Contacto básico
      client_phone,
      email_id,
      
      -- Auditoría
      file_date,
      current_timestamp() as created_at
      
    FROM {BRONZE_TB_RAW_CLIENTES}
    WHERE cust_code IS NOT NULL
      AND active_flg = true
    """)
    
    # Diccionario de comentarios
    column_comments = {
        'cust_code': 'Código único del cliente',
        'no_cli': 'Número de cliente',
        'brand_name': 'Marca o nombre comercial',
        'client_desc': 'Descripción del cliente',
        'active_flg': 'Flag de cliente activo',
        'available_credit': 'Crédito disponible',
        'credit_line': 'Línea de crédito asignada',
        'credit_utilization_pct': 'Porcentaje de utilización del crédito',
        'created_ts': 'Fecha de creación del cliente',
        'days_since_creation': 'Días de antigüedad desde creación',
        'country_name': 'País del cliente',
        'region_name': 'Región del cliente',
        'province_name': 'Provincia del cliente',
        'district_name': 'Distrito del cliente',
        'zone_name': 'Zona geográfica',
        'client_address': 'Dirección del cliente',
        'zip_code': 'Código postal',
        'client_phone': 'Teléfono del cliente',
        'email_id': 'Correo electrónico',
        'file_date': 'Fecha del archivo fuente',
        'created_at': 'Timestamp de creación del registro'
    }
    
    # Agregar comentarios
    for column, comment in column_comments.items():
        try:
            spark.sql(f"""
            ALTER TABLE {SILVER_MV_CLIENTES_SIMPLE} 
            ALTER COLUMN {column} COMMENT '{comment}'
            """)
        except Exception as e:
            print(f"⚠️ No se pudo agregar comentario para {column}: {e}")
    
    print(f"✅ Vista materializada {SILVER_MV_CLIENTES_SIMPLE} creada exitosamente")

# Ejecutar
create_clientes_materialized_view()