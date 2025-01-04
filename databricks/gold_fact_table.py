# Databricks notebook source
# MAGIC %md
# MAGIC #Creation of Fact Table

# COMMAND ----------

# MAGIC %md 
# MAGIC Reading Silver Data

# COMMAND ----------


df_silver = spark.sql("select * from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all DIMS**

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")
df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")
df_model = spark.sql("select * from cars_catalog.gold.dim_model")
df_date = spark.sql("select * from cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Brinnging keys to fact table
# MAGIC

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, "left").join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, "left").join(df_model, df_silver.Model_ID == df_model.Model_ID, "left").join(df_date, df_silver.Date_ID == df_date.Date_ID, "left").select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver["Revenue_Per_Unit"],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Fact Table

# COMMAND ----------

from delta.tables import *

if spark.catalog.tableExists('fact_sales'):
    delta_table = DeltaTable.forName(spark, 'cars_catalog.gold.fact_sales')
    delta_table.alias("target").merge(df_fact.alias("source"), "target.dim_branch_key = source.dim_branch_key and target.dim_date_key = source.dim_date_key and target.dim_dealer_key = source.dim_dealer_key and target.dim_model_key = source.dim_model_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact.write.format('delta').mode('append').mode("overwrite").option("path","abfss://gold@salesprojectstg.dfs.core.windows.net/fact_sales").saveAsTable('cars_catalog.gold.fact_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.fact_sales

# COMMAND ----------

# Read the table from the catalog and schema
duplicate_df = spark.read.table("cars_catalog.gold.fact_sales")

# Drop duplicates based on all columns
df_cleaned = duplicate_df.dropDuplicates()

# Write back to the table (overwrite the existing table or create a new one)
df_cleaned.write.mode("overwrite").saveAsTable("cars_catalog.gold.fact_sales")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cars_catalog.gold.fact_sales

# COMMAND ----------


