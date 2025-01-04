# Databricks notebook source
# MAGIC %md
# MAGIC #catalog creation

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC schema creation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;
# MAGIC create schema cars_catalog.gold;

# COMMAND ----------


