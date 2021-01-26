# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ETL Explained
# MAGIC 
# MAGIC * read from / write to files
# MAGIC * slice, select, filter
# MAGIC * aggregation - group by
# MAGIC * sort
# MAGIC * join
# MAGIC 
# MAGIC * subqeries
# MAGIC * UDF

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/samples/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/databricks-datasets/samples/lending_club/parquet

# COMMAND ----------

path='/databricks-datasets/samples/lending_club/parquet'
df = spark.read.format('parquet').load(path)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_silver = (
  df
  .select('addr_state', 'loan_status', 'loan_amnt', 'int_rate')
  .withColumnRenamed('addr_state', 'state')
)

display(df_silver)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df2 = (
  df
#  .filter(col('int_rate') < 10.0)
#  .filter(df.int_rate < 10.0)
  .withColumn(
    'int_rate_double', 
    regexp_replace('int_rate', '%', '').cast('double')
  )
  .filter(col('int_rate_double') < 10.0)
  .select(col('addr_state').alias('state'), col('loan_status'), col('loan_amnt'), col('int_rate'), col('int_rate_double') )
)

display(df2)

# COMMAND ----------

