# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook **will run** in Pipeline
# MAGIC - Need to create temporary views and insert into fixed raw table
# MAGIC - COMMENT widget creation: dbutils.widgets.text() -> It will be created in the pipeline

# COMMAND ----------

from datetime import datetime, date

# COMMAND ----------

dbutils.widgets.text('BRONZE_TB_RAW_PEDIDOS', 'g6_cmc_pedidos.bronze.raw_pedidos')


# COMMAND ----------

BRONZE_TB_RAW_PEDIDOS = dbutils.widgets.get("BRONZE_TB_RAW_PEDIDOS")


# COMMAND ----------

print('BRONZE_TB_RAW_PEDIDOS\t:', BRONZE_TB_RAW_PEDIDOS)


# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS g6_cmc_pedidos.bronze.raw_pedidos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE g6_cmc_pedidos.bronze.raw_pedidos (
# MAGIC     -- Identificadores principales
# MAGIC     proj_code BIGINT COMMENT 'Código del proyecto',
# MAGIC     order_date TIMESTAMP COMMENT 'Fecha y hora del pedido',
# MAGIC     order_code BIGINT COMMENT 'Código único del pedido',
# MAGIC     
# MAGIC     -- Información del cliente
# MAGIC     cust_code STRING COMMENT 'Código/nombre del cliente',
# MAGIC     no_cli STRING COMMENT 'Número/código corto del cliente',
# MAGIC     
# MAGIC     -- Información de la obra
# MAGIC     co_obr STRING COMMENT 'Código de la obra',
# MAGIC     de_obr STRING COMMENT 'Descripción/nombre de la obra',
# MAGIC     obra_latitud DECIMAL(12,8) COMMENT 'Latitud de la obra',
# MAGIC     obra_longitud DECIMAL(12,8) COMMENT 'Longitud de la obra',
# MAGIC     start_time TIMESTAMP COMMENT 'Hora de inicio programada',
# MAGIC     
# MAGIC     -- Información del producto
# MAGIC     prod_code BIGINT COMMENT 'Código del producto',
# MAGIC     short_prod_descr STRING COMMENT 'Descripción corta del producto',
# MAGIC     elemento STRING COMMENT 'Tipo de elemento (Losa, Escalera, Columna, etc.)',
# MAGIC     frecuencia INT COMMENT 'Frecuencia del producto',
# MAGIC     
# MAGIC     -- Cantidades
# MAGIC     tamano_de_carga DECIMAL(10,2) COMMENT 'Tamaño de la carga',
# MAGIC     order_qty DECIMAL(10,2) COMMENT 'Cantidad pedida',
# MAGIC     delv_qty DECIMAL(10,2) COMMENT 'Cantidad entregada',
# MAGIC     
# MAGIC     -- Tiempos promedio (en minutos o la unidad correspondiente)
# MAGIC     promedio_tiempo_a_obra DECIMAL(12,2) COMMENT 'Tiempo promedio para llegar a obra',
# MAGIC     promedio_tiempo_espera DECIMAL(10,2) COMMENT 'Tiempo promedio de espera',
# MAGIC     promedio_tiempo_vaciado DECIMAL(10,2) COMMENT 'Tiempo promedio de vaciado',
# MAGIC     promedio_tiempo_salida DECIMAL(12,2) COMMENT 'Tiempo promedio de salida',
# MAGIC     promedio_tiempo_a_planta DECIMAL(12,2) COMMENT 'Tiempo promedio de regreso a planta',
# MAGIC     
# MAGIC     -- Metadatos del procesamiento
# MAGIC     file_date DATE COMMENT 'Fecha del archivo procesado',
# MAGIC     source_file_path STRING COMMENT 'Ruta del archivo fuente',
# MAGIC     inserted_at TIMESTAMP COMMENT 'Timestamp de inserción en la tabla'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (file_date)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'description' = 'Tabla bronze con datos raw de pedidos del sistema G06',
# MAGIC     'created_by' = 'data_pipeline_g06'
# MAGIC );

# COMMAND ----------

from datetime import datetime
import re

def get_date_from_filename(file_path):
    """
    Extrae la fecha del nombre del archivo.
    Ejemplo: pedidos_20240101.csv -> 2024-01-01
    """
    match = re.search(r'pedidos_(\d{8})\.csv', file_path)
    if match:
        date_str = match.group(1)
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d').date()
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            return None
    return None

def table_exists(bronze_table, spark_session):
    """
    Verifica si la tabla existe.
    """
    try:
        spark_session.sql(f"DESCRIBE {bronze_table}").collect()
        return True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
            return False
        else:
            # Si es otro tipo de error, lo reportamos
            print(f"⚠️ Error inesperado verificando tabla: {str(e)}")
            return False

def create_table_from_first_file(bronze_table, first_file_path, first_file_date, spark_session):
    """
    Crea la tabla usando el primer archivo como referencia para el schema.
    """
    try:
        print(f"🏗️ Creando tabla {bronze_table} usando {first_file_path.split('/')[-1]} como referencia...")
        
        # Crear tabla con schema inferido del primer archivo
        create_table_sql = f"""
        CREATE TABLE {bronze_table}
        USING DELTA
        AS
        SELECT 
            CAST(proj_code AS BIGINT) as proj_code,
            CAST(order_date AS TIMESTAMP) as order_date,
            CAST(order_code AS BIGINT) as order_code,
            CAST(cust_code AS STRING) as cust_code,
            CAST(no_cli AS STRING) as no_cli,
            CAST(co_obr AS STRING) as co_obr,
            CAST(de_obr AS STRING) as de_obr,
            CAST(obra_latitud AS DECIMAL(12,8)) as obra_latitud,
            CAST(obra_longitud AS DECIMAL(12,8)) as obra_longitud,
            CAST(start_time AS TIMESTAMP) as start_time,
            CAST(prod_code AS BIGINT) as prod_code,
            CAST(short_prod_descr AS STRING) as short_prod_descr,
            CAST(elemento AS STRING) as elemento,
            CAST(frecuencia AS INT) as frecuencia,
            CAST(`tamaño_de_carga` AS DECIMAL(10,2)) as tamano_de_carga,
            CAST(order_qty AS DECIMAL(10,2)) as order_qty,
            CAST(delv_qty AS DECIMAL(10,2)) as delv_qty,
            CAST(promedio_tiempo_a_obra AS DECIMAL(12,2)) as promedio_tiempo_a_obra,
            CAST(promedio_tiempo_espera AS DECIMAL(10,2)) as promedio_tiempo_espera,
            CAST(promedio_tiempo_vaciado AS DECIMAL(10,2)) as promedio_tiempo_vaciado,
            CAST(promedio_tiempo_salida AS DECIMAL(12,2)) as promedio_tiempo_salida,
            CAST(promedio_tiempo_a_planta AS DECIMAL(12,2)) as promedio_tiempo_a_planta,
            '{first_file_date}' as file_date,
            '{first_file_path}' as source_file_path,
            current_timestamp() AS inserted_at 
        FROM read_files(
            '{first_file_path}',
            format => 'csv',
            header => true,
            inferSchema => false
        )
        """
        
        spark_session.sql(create_table_sql)
        
        # Verificar cuántos registros se insertaron
        count = spark_session.sql(f"SELECT COUNT(*) as count FROM {bronze_table}").collect()[0]['count']
        print(f"✅ Tabla {bronze_table} creada exitosamente con {count} registros")
        
        return True, count
        
    except Exception as e:
        print(f"❌ Error creando tabla: {str(e)}")
        return False, 0

def process_all_files_auto_create(bronze_table, base_path, spark_session=None):
    """
    Procesa todos los archivos CSV, creando la tabla automáticamente si no existe.
    
    Args:
        bronze_table: Nombre de la tabla bronze destino
        base_path: Ruta base donde se encuentran los archivos
        spark_session: Sesión de Spark (opcional)
    
    Returns:
        dict: Resumen del procesamiento
    """
    
    # Usar sesión de Spark activa
    if spark_session is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No hay sesión de Spark activa")
    else:
        spark = spark_session
    
    print(f"🔍 Buscando archivos en: {base_path}")
    
    # Buscar todos los archivos CSV de pedidos
    try:
        search_path = f"{base_path}/pedidos_*.csv"
        
        # Obtener lista de archivos disponibles (compatible con Unity Catalog)
        files_query = f"""
        SELECT _metadata.file_path as file_path, COUNT(*) as row_count
        FROM read_files(
            '{search_path}',
            format => 'csv',
            header => true,
            inferSchema => false
        )
        GROUP BY _metadata.file_path
        ORDER BY _metadata.file_path
        """
        
        files_df = spark.sql(files_query)
        available_files = files_df.collect()
        
        if not available_files:
            print("❌ No se encontraron archivos con patrón 'pedidos_*.csv'")
            return {
                'total_found': 0,
                'processed': 0,
                'skipped': 0,
                'failed': 0,
                'table_created': False,
                'results': []
            }
        
        print(f"📋 Se encontraron {len(available_files)} archivos")
        
    except Exception as e:
        print(f"❌ Error buscando archivos: {str(e)}")
        return {
            'total_found': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'table_created': False,
            'results': [],
            'error': str(e)
        }
    
    # Verificar si la tabla existe
    table_exists_flag = table_exists(bronze_table, spark)
    table_created = False
    
    if not table_exists_flag:
        print(f"📋 La tabla {bronze_table} no existe. Se creará automáticamente.")
        
        # Usar el primer archivo para crear la tabla
        first_file = available_files[0]
        first_file_path = first_file['file_path']
        first_file_date = get_date_from_filename(first_file_path)
        
        if not first_file_date:
            print(f"❌ No se pudo extraer fecha del primer archivo: {first_file_path}")
            return {
                'total_found': len(available_files),
                'processed': 0,
                'skipped': 0,
                'failed': 1,
                'table_created': False,
                'results': [],
                'error': 'No se pudo extraer fecha del primer archivo'
            }
        
        success, first_count = create_table_from_first_file(bronze_table, first_file_path, first_file_date, spark)
        
        if not success:
            return {
                'total_found': len(available_files),
                'processed': 0,
                'skipped': 0,
                'failed': 1,
                'table_created': False,
                'results': [],
                'error': 'Error creando tabla'
            }
        
        table_created = True
        # Remover el primer archivo de la lista ya que ya fue procesado
        remaining_files = available_files[1:]
        processed = 1
        first_result = {
            'file': first_file_path.split('/')[-1],
            'date': first_file_date,
            'status': 'success',
            'records_processed': first_count,
            'note': 'Usado para crear tabla'
        }
        results = [first_result]
    else:
        print(f"✅ La tabla {bronze_table} ya existe")
        remaining_files = available_files
        processed = 0
        results = []
    
    # Procesar archivos restantes
    skipped = 0
    failed = 0
    
    for file_info in remaining_files:
        file_path = file_info['file_path']
        file_name = file_path.split('/')[-1]
        
        print(f"\n{'='*50}")
        print(f"🔄 Procesando: {file_name}")
        
        # Extraer fecha del nombre del archivo
        file_date = get_date_from_filename(file_path)
        if not file_date:
            print(f"⚠️ No se pudo extraer fecha de {file_name}")
            failed += 1
            results.append({
                'file': file_name,
                'status': 'failed',
                'error': 'No se pudo extraer fecha del nombre'
            })
            continue
        
        print(f"📅 Fecha detectada: {file_date}")
        
        # Verificar si ya existe data para esta fecha
        try:
            existing_count = spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM {bronze_table} 
                WHERE file_date = '{file_date}'
            """).collect()[0]['count']
            
            if existing_count > 0:
                print(f"⏭️ Ya procesado. Registros existentes: {existing_count}")
                skipped += 1
                results.append({
                    'file': file_name,
                    'date': file_date,
                    'status': 'skipped',
                    'existing_records': existing_count
                })
                continue
                
        except Exception as e:
            print(f"❌ Error verificando datos existentes: {str(e)}")
            failed += 1
            results.append({
                'file': file_name,
                'date': file_date,
                'status': 'failed',
                'error': f'Error verificando datos: {str(e)}'
            })
            continue
        
        # Procesar archivo
        try:
            print(f"📊 Procesando datos...")
            
            # Crear vista temporal con conversiones explícitas de tipos
            spark.sql(f"""
                CREATE OR REPLACE TEMPORARY VIEW archivo_actual AS
                SELECT 
                    CAST(proj_code AS BIGINT) as proj_code,
                    CAST(order_date AS TIMESTAMP) as order_date,
                    CAST(order_code AS BIGINT) as order_code,
                    CAST(cust_code AS STRING) as cust_code,
                    CAST(no_cli AS STRING) as no_cli,
                    CAST(co_obr AS STRING) as co_obr,
                    CAST(de_obr AS STRING) as de_obr,
                    CAST(obra_latitud AS DECIMAL(12,8)) as obra_latitud,
                    CAST(obra_longitud AS DECIMAL(12,8)) as obra_longitud,
                    CAST(start_time AS TIMESTAMP) as start_time,
                    CAST(prod_code AS BIGINT) as prod_code,
                    CAST(short_prod_descr AS STRING) as short_prod_descr,
                    CAST(elemento AS STRING) as elemento,
                    CAST(frecuencia AS INT) as frecuencia,
                    CAST(`tamaño_de_carga` AS DECIMAL(10,2)) as tamano_de_carga,
                    CAST(order_qty AS DECIMAL(10,2)) as order_qty,
                    CAST(delv_qty AS DECIMAL(10,2)) as delv_qty,
                    CAST(promedio_tiempo_a_obra AS DECIMAL(12,2)) as promedio_tiempo_a_obra,
                    CAST(promedio_tiempo_espera AS DECIMAL(10,2)) as promedio_tiempo_espera,
                    CAST(promedio_tiempo_vaciado AS DECIMAL(10,2)) as promedio_tiempo_vaciado,
                    CAST(promedio_tiempo_salida AS DECIMAL(12,2)) as promedio_tiempo_salida,
                    CAST(promedio_tiempo_a_planta AS DECIMAL(12,2)) as promedio_tiempo_a_planta,
                    '{file_date}' as file_date,
                    '{file_path}' as source_file_path,
                    current_timestamp() AS inserted_at 
                FROM read_files(
                    '{file_path}',
                    format => 'csv',
                    header => true,
                    inferSchema => false
                )
            """)
            
            # Contar registros
            record_count = spark.sql("SELECT COUNT(*) as count FROM archivo_actual").collect()[0]['count']
            
            if record_count == 0:
                print(f"⚠️ El archivo no contiene datos")
                failed += 1
                results.append({
                    'file': file_name,
                    'date': file_date,
                    'status': 'failed',
                    'error': 'Archivo sin datos'
                })
                continue
            
            print(f"📈 Registros a insertar: {record_count}")
            
            # Insertar datos
            spark.sql(f"""
                INSERT INTO {bronze_table}
                SELECT * FROM archivo_actual
            """)
            
            # Verificar inserción
            inserted_count = spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM {bronze_table} 
                WHERE file_date = '{file_date}'
            """).collect()[0]['count']
            
            print(f"✅ Procesado exitosamente. Registros insertados: {inserted_count}")
            processed += 1
            
            results.append({
                'file': file_name,
                'date': file_date,
                'status': 'success',
                'records_processed': inserted_count
            })
            
        except Exception as e:
            print(f"❌ Error procesando archivo: {str(e)}")
            failed += 1
            results.append({
                'file': file_name,
                'date': file_date,
                'status': 'failed',
                'error': str(e)
            })
            
            # Limpiar vista temporal en caso de error
            try:
                spark.sql("DROP VIEW IF EXISTS archivo_actual")
            except:
                pass
    
    # Resumen final
    print(f"\n{'='*60}")
    print(f"📊 RESUMEN FINAL")
    print(f"📋 Archivos encontrados: {len(available_files)}")
    if table_created:
        print(f"🏗️ Tabla creada automáticamente: SÍ")
    print(f"✅ Procesados exitosamente: {processed}")
    print(f"⏭️ Ya existían (saltados): {skipped}")
    print(f"❌ Fallaron: {failed}")
    
    # Mostrar total de registros en la tabla
    try:
        total_records = spark.sql(f"SELECT COUNT(*) as count FROM {bronze_table}").collect()[0]['count']
        print(f"📈 Total registros en tabla: {total_records}")
    except:
        pass
    
    print(f"{'='*60}")
    
    return {
        'total_found': len(available_files),
        'processed': processed,
        'skipped': skipped,
        'failed': failed,
        'table_created': table_created,
        'results': results
    }

# Función simple para usar
def process_pedidos_folder_auto(bronze_table, base_path):
    """
    Función simplificada que crea la tabla automáticamente si no existe.
    
    Args:
        bronze_table: Nombre de la tabla bronze (ej: 'bronze.raw_pedidos')
        base_path: Ruta de la carpeta (ej: 'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/g06/pedidos')
    """
    
    print("🚀 Iniciando procesamiento de carpeta de pedidos (con auto-creación de tabla)...")
    
    summary = process_all_files_auto_create(bronze_table, base_path)
    
    if summary['table_created']:
        print(f"🎉 ¡Tabla creada y {summary['processed']} archivos procesados!")
    elif summary['processed'] > 0:
        print(f"🎉 Procesamiento completado! Se procesaron {summary['processed']} archivos nuevos")
    else:
        print("ℹ️ No se procesaron archivos nuevos")
    
    return summary

# Ejemplo de uso
if __name__ == "__main__":
    # Configuración - USAR EL NOMBRE COMPLETO DE LA TABLA
    BRONZE_TABLE = "g6_cmc_pedidos.bronze.raw_pedidos"
    BASE_PATH = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/g06/pedidos"
    
    # Ejecutar procesamiento
    result = process_pedidos_folder_auto(BRONZE_TABLE, BASE_PATH)