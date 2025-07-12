# Databricks notebook source
# MAGIC %run /Shared/data_platform/data_contracts

# COMMAND ----------

help(get_data_contract)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Contract: Puntualidad Obra

# COMMAND ----------

data_contract = get_data_contract(
    catalog_name = 'g6_ops_despachos',
    schema_name = 'gold',
    table_name = 'vw_ops_puntualidad_obra',
    data_product_name = 'despachos',
    domain_name = 'operaciones',
    owner_email = 'angel.tintaya@utec.edu.pe',
    freshness = 'updated_monthly',
    rules_field_not_null = ['order_id'],
    rules_field_email = None,
    rules_field_dob = None
    )

# COMMAND ----------

print(data_contract)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Contract: Puntualidad Cliente

# COMMAND ----------

data_contract = get_data_contract(
    catalog_name = 'g6_ops_despachos',
    schema_name = 'gold',
    table_name = 'vw_ops_puntualidad_cliente',
    data_product_name = 'despachos',
    domain_name = 'operaciones',
    owner_email = 'angel.tintaya@utec.edu.pe',
    freshness = 'updated_daily',
    rules_field_not_null = ['order_id'],
    rules_field_email = None,
    rules_field_dob = None
    )


# COMMAND ----------

print(data_contract)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Contract: UM Puntualidad Planta

# COMMAND ----------

data_contract = get_data_contract(
    catalog_name = 'g6_ops_despachos',
    schema_name = 'gold',
    table_name = 'vw_ops_puntualidad_planta',
    data_product_name = 'despachos',
    domain_name = 'operaciones',
    owner_email = 'angel.tintaya@utec.edu.pe',
    freshness = 'updated_daily',
    rules_field_not_null = ['order_id'],
    rules_field_email = None,
    rules_field_dob = None
    )



# COMMAND ----------

print(data_contract)