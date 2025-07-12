# Databricks notebook source
# MAGIC %md
# MAGIC # Creación de Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS g6_cmc_pedidos.bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS g6_cmc_pedidos.silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS g6_cmc_pedidos.gold CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE g6_cmc_pedidos.bronze;
# MAGIC CREATE DATABASE g6_cmc_pedidos.silver;
# MAGIC CREATE DATABASE g6_cmc_pedidos.gold;