# Databricks notebook source
# MAGIC %md
# MAGIC #data Reading

# COMMAND ----------

df = spark.read.format("parquet").option('inferSchema',True).load("abfss://bronze@salesprojectstg.dfs.core.windows.net/dataset/*.parquet",header=True)

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Transformations
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
df = df.withColumn('model_name',split(col('Model_ID'),'-')[0])
display(df)

# COMMAND ----------

from pyspark.sql.types import *
df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

df=df.withColumn('revenue_per_unit', col('Revenue')/col("Units_Sold"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #ad-hoc

# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias("Total_Units_Sold")).sort('Year','Total_Units_Sold',ascending=[True,False]))

# COMMAND ----------

#data writing
df.write.format("parquet").mode("append").option("path","abfss://silver@salesprojectstg.dfs.core.windows.net/carsales/").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC --quering silver_data
# MAGIC select * from parquet.`abfss://silver@salesprojectstg.dfs.core.windows.net/carsales`

# COMMAND ----------


