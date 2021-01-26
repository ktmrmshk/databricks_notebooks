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

# MAGIC %fs ls /databricks-datasets/airlines/part-00659

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC head /dbfs/databricks-datasets/airlines/part-00659

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

df_airline = spark.read.format('csv').load('/databricks-datasets/airlines/*')
df_airline.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %fs ls /home/masahiko.kitamura@databricks.com/

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('dbfs:/home/masahiko.kitamura@databricks.com/airlines')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %sh 
# MAGIC du -sh /dbfs/databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -l /dbfs/home/masahiko.kitamura@databricks.com/airlines/

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/home/masahiko.kitamura@databricks.com/airlines/

# COMMAND ----------

display(df)

# COMMAND ----------

df2_sum = df.groupby('issue_d').count().orderBy('issue_d')
df2_sum = df2_sum.filter(col('issue_d').contains('-'))
display(df2_sum)

# COMMAND ----------

from pyspark.sql.functions import split, month, date_format, to_timestamp, year

df3 = (
  df2_sum
#  .withColumn('month', split(col('issue_d'), '-').getItem(0))
#  .withColumn('year', split(col('issue_d'), '-').getItem(1))
#  .withColumn('month_num', month('month'))
  .withColumn('dat', to_timestamp('issue_d', 'MMM-yyyy'))
  .withColumn('month', month('dat'))
  .withColumn('year', year('dat'))
  .select('count', 'year', 'month', 'dat')
  .orderBy('dat')
)
display(df3)

# COMMAND ----------

