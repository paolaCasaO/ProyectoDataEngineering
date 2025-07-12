# Databricks notebook source
# MAGIC %md
# MAGIC # Creación de Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS g6_ops_despachos.bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS g6_ops_despachos.silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS g6_ops_despachos.gold CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE g6_ops_despachos.bronze;
# MAGIC CREATE DATABASE g6_ops_despachos.silver;
# MAGIC CREATE DATABASE g6_ops_despachos.gold;