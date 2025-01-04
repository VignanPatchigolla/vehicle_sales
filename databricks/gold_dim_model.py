# Databricks notebook source
# MAGIC %md
# MAGIC #create flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md 
# MAGIC #Creating Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`

# COMMAND ----------

df_src=spark.sql('''select distinct(model_id) as model_id, model_name from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model_sink -Intial and incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('''select dim_model_key,model_id,model_name from cars_catalog.gold.dim_model''')
else:
    df_sink = spark.sql('''select 1 as dim_model_key,model_id,model_name from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales` where 1 = 0''')

# COMMAND ----------

# MAGIC %md
# MAGIC ###filtering new and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.model_id == df_sink.model_id, 'left').select(df_src['Model_ID'],df_src['Model_Name'],df_sink['dim_model_key'])
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_model_key'].isNotNull())

# COMMAND ----------

#df_filter_new
df_filter_new = df_filter.filter(df_filter['dim_model_key'].isNull()).select(df_filter['Model_ID'],df_filter['Model_Name'])

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
    max_value_df = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0] + 1

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add max surrogate key

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df_filter_new = df_filter_new.withColumn('dim_model_key', max_value + monotonically_increasing_id())
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
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@salesprojectstg.dfs.core.windows.net/dim_model")

    delta_table.alias("target").merge(df_final.alias("source"),"target.dim_model_key = source.dim_model_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#initial Run case
else: 
    df_final.write.format("Delta").mode("append").option("path","abfss://gold@salesprojectstg.dfs.core.windows.net/dim_model").saveAsTable("cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------


