# Databricks notebook source
# MAGIC %md
# MAGIC #create flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md 
# MAGIC #Creating Dimension Branch

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src=spark.sql('''select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_dealer_sink -Intial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('''select dim_dealer_key,Dealer_Id,DealerName from cars_catalog.gold.dim_dealer''')
else:
    df_sink = spark.sql('''select 1 as dim_dealer_key,Dealer_id,DealerName from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales` where 1 = 0''')

# COMMAND ----------

# MAGIC %md
# MAGIC ###filtering new and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID'] == df_sink['Dealer_ID'], 'left').select(df_src['Dealer_ID'],df_src['DealerName'],df_sink['dim_dealer_key'])
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_dealer_key'].isNotNull())

# COMMAND ----------

#df_filter_new
df_filter_new = df_filter.filter(df_filter['dim_dealer_key'].isNull()).select(df_filter['Dealer_ID'],df_filter['DealerName'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###creation of surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch max_surrogate key from existing table

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0] + 1

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add max surrogate key

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value + monotonically_increasing_id())
display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create final_df

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 1(UPDATE/UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable
#incremental Run case
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@salesprojectstg.dfs.core.windows.net/dim_dealer")

    delta_table.alias("target").merge(df_final.alias("source"),"target.dim_dealer_key = source.dim_dealer_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#initial Run case
else: 
    df_final.write.format("Delta").mode("append").option("path","abfss://gold@salesprojectstg.dfs.core.windows.net/dim_dealer").saveAsTable("cars_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------


