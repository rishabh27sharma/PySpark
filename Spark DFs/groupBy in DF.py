# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("groupBy in DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

# first group the data and then you must perform some aggregation(sum, min, max, avg, count)
df.groupBy("gender").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").count().show()  
df.groupBy("course").count().show() 
df.groupBy("course").sum("marks").show()

# COMMAND ----------

df.groupBy("gender").max("marks").show()
df.groupBy("gender").min("marks").show()

# COMMAND ----------

df.groupBy("age").avg("marks").show()

# COMMAND ----------

df.groupBy("gender").mean("marks").show()

# COMMAND ----------


