# Databricks notebook source
# MAGIC %run /Shared/data_platform/data_contracts

# COMMAND ----------

help(get_data_contract)

# COMMAND ----------

[t.name for t in spark.catalog.listTables("g6_cmc_pedidos.gold")]

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Contract: Demanda de pedidos 

# COMMAND ----------

data_contract = get_data_contract(
    catalog_name = 'g6_cmc_pedidos',
    schema_name = 'gold',
    table_name = 'vw_demanda_pedidos',
    data_product_name = 'pedidos',
    domain_name = 'comercial',
    owner_email = 'angel.tintaya@utec.edu.pe',
    freshness = 'updated_daily',
    rules_field_not_null = ['order_id', 'cust_code'],
    rules_field_email = None,
    rules_field_dob = None
    )

# COMMAND ----------

print(data_contract)